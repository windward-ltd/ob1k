package com.outbrain.ob1k.consul;

import static java.util.stream.Collectors.toList;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.collect.Ordering;
import com.outbrain.ob1k.client.targets.TargetProvider;
import com.outbrain.ob1k.concurrent.handlers.FutureAction;

/**
 * A {@link TargetProvider} that chooses the targets most likely to respond fast and successfully.
 *
 * @author Amir Hadadi
 */
public class CooperativeTargetProviderNoContention extends ConsulBasedTargetProvider {

  private volatile ConcurrentMap<String, LongAdder> pendingRequests = new ConcurrentHashMap<>();
  private final Set<String> failingTargets = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final LongAdder roundRobin = new LongAdder();
  private final int extraTargets;

  public CooperativeTargetProviderNoContention(final HealthyTargetsList healthyTargetsList,
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
    if (failingTargets.contains(firstTarget) && pendingRequests(firstTarget) == 0) {
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
    roundRobin.increment();
    final long index = roundRobin.sum();

    // limit the target pool size to keep the complexity O(targetsNum).
    final int targetsPoolSize = Math.min(targetsNum + extraTargets, Math.max(targetsNum, targets.size()));
    final List<String> targetsPool = new ArrayList<>(targetsPoolSize);
    for (int i = 0; i < targetsPoolSize; i++) {
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
      pendingRequests[i] = pendingRequests(target);
      failures[i] = failingTargets.contains(target);
    }

    final List<Integer> indexes = new AbstractList<Integer>() {
      @Override
      public Integer get(final int index) {
        return index;
      }

      @Override
      public int size() {
        return poolSize;
      }
    };

    return Ordering.from(Comparator.
            // Successful targets come before failing targets.
            <Integer, Boolean>comparing(index -> failures[index]).
            // Targets with low number of pending requests come before targets with a high number.
            thenComparingInt(index -> pendingRequests[index]).
            // Tie break by round robin order.
            thenComparingInt(x -> x)).
            leastOf(indexes, targetsNum).
            stream().
            map(targetsPool::get).
            collect(toList());
  }

  private int pendingRequests(final String target) {
    final ConcurrentMap<String, LongAdder> pendingRequests = this.pendingRequests;
    final LongAdder longAdder = pendingRequests.get(target);
    return longAdder == null ? 0 : longAdder.intValue();
  }

  @Override
  protected void notifyTargetsChanged(final List<String> targets) {
    pendingRequests = new ConcurrentHashMap<>();
    failingTargets.retainAll(targets);
  }

  public void targetDispatched(final String target, FutureAction<?> action) {
    final ConcurrentMap<String, LongAdder> pendingRequests = this.pendingRequests;
    LongAdder tmpCount = pendingRequests.get(target);
    if (tmpCount == null) {
      tmpCount = pendingRequests.computeIfAbsent(target, t -> new LongAdder());
    }
    final LongAdder count = tmpCount;
    count.increment();
    action.execute().andThen(t -> {
      count.decrement();
      if (t.isSuccess()) {
        failingTargets.remove(target);
      } else {
        failingTargets.add(target);
      }
    });
  }

  public void targetDispatched(String t1) {

  }

  public void targetDispatchEnded(String t1, boolean b) {

  }
}
