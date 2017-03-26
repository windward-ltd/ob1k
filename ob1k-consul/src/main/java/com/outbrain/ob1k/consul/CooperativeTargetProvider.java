package com.outbrain.ob1k.consul;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.outbrain.ob1k.client.targets.TargetProvider;

/**
 * A {@link TargetProvider} that chooses the targets most likely to respond fast and successfully.
 *
 * @author Amir Hadadi
 */
public class CooperativeTargetProvider extends ConsulBasedTargetProvider {

  private final Multiset<String> pendingRequests = ConcurrentHashMultiset.create();
  private final Set<String> failingTargets = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final AtomicLong roundRobin = new AtomicLong();
  private final int extraTargets;

  public CooperativeTargetProvider(final HealthyTargetsList healthyTargetsList,
                                   final String urlSuffix,
                                   final Map<String, Integer> tag2weight,
                                   final int extraTargets) {
    super(healthyTargetsList, urlSuffix, tag2weight);
    this.extraTargets = extraTargets;
  }

  @Override
  public List<String> provideTargetsImpl(final int targetsNum, final List<String> targets) {
    final List<String> targetsPool = createTargetsPool(targetsNum, targets);

    final String firstTarget = targetsPool.get(0);

    // We dispatch to a failing target if we got to it in the round robin order
    // and it has no pending requests.
    if (failingTargets.contains(firstTarget) && !pendingRequests.contains(firstTarget)) {
      final List<String> providedTargets = new ArrayList<>(targetsNum);
      providedTargets.add(firstTarget);
      providedTargets.addAll(bestTargets(targetsNum - 1, targetsPool.subList(1, targetsPool.size())));
      return providedTargets;
    } else {
      return bestTargets(targetsNum, targetsPool);
    }
  }

  private List<String> createTargetsPool(final int targetsNum, final List<String> targets) {
    // We assume that only the first target is likely to be used, the other targets
    // may serve for double dispatch. That's why we increment the round robin counter
    // by one instead of targetsNum.
    final long index = roundRobin.getAndIncrement();

    // limit the target pool size to keep the complexity O(targetsNum + extraTargets).
    final int targetsPoolSize = Math.min(targetsNum + extraTargets, Math.max(targetsNum, targets.size()));

    final List<String> targetsPool = new ArrayList<>(targetsPoolSize);
    for (int i=0; i<targetsPoolSize; i++) {
      // Using long for round robin to avoid wrapping, so that we get a distinct list of targets
      // in case targetsNum <= targets.size().
      targetsPool.add(targets.get((int)((index + i) % targets.size())));
    }
    return targetsPool;
  }

  private List<String> bestTargets(final int targetsNum, final List<String> targetsPool) {
    if (targetsNum == 0) {
      return Collections.emptyList();
    }

    // create snapshots so that the comparator will not switch the order of targets during sorting.
    final int poolSize = targetsPool.size();
    final int[] pendingRequests = new int[poolSize];
    final boolean[] failures = new boolean[poolSize];
    for (int i = 0; i < poolSize; i++) {
      final String target = targetsPool.get(i);
      pendingRequests[i] = this.pendingRequests.count(target);
      failures[i] = failingTargets.contains(target);
    }

    final List<Integer> indexes = new ArrayList<>(poolSize);
    for (int i=0; i<poolSize; i++) {
      indexes.add(i);
    }

    final List<Integer> bestIndices = Ordering.from(Comparator.
            // Successful targets come before failing targets.
                    <Integer, Boolean>comparing(index -> failures[index]).
            // Targets with low number of pending requests come before targets with a high number.
                    thenComparingInt(index -> pendingRequests[index]).
            // Tie break by round robin order.
                    thenComparingInt(x -> x)).
            leastOf(indexes, targetsNum);

    // Lists.transform is significantly faster then stream + collect to list.
    return Lists.transform(bestIndices, targetsPool::get);
  }

  @Override
  protected void notifyTargetsChanged(final List<String> targets) {
    failingTargets.retainAll(targets);
  }

  public void targetDispatched(final String target) {
    pendingRequests.add(target);
  }

  public void targetDispatchEnded(final String target, final boolean success) {
    pendingRequests.remove(target);
    if (success) {
      failingTargets.remove(target);
    } else {
      failingTargets.add(target);
    }
  }
}
