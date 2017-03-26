package com.outbrain.ob1k.consul;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by ahadadi on 18/03/2017.
 */
@RunWith(MockitoJUnitRunner.class)
public class CooperativeTargetProviderTest {

  @Mock
  private HealthyTargetsList healthyTargetsList;

  private CooperativeTargetProvider provider;

  @Before
  public void setup() {
    provider = new CooperativeTargetProvider(healthyTargetsList, "urlSuffix", Collections.singletonMap("tag", 1), 2);
  }

  @Ignore
  @Test
  public void testReturnByNumberOfPendingRequests2() throws InterruptedException {
    final List<String> targets = IntStream.range(0, 3).mapToObj(i -> ("t" + i)).collect(Collectors.toList());

    final int nThreads = 1;
    final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    for (int j=0; j<nThreads; j++)
      executorService.execute(() -> {
        provider.notifyTargetsChanged(targets);
        for (int i=0; i < 25_000_000; i++) {
          final String target = provider.provideTargetsImpl(2, targets).get(0);
          provider.targetDispatched(target);
          provider.targetDispatchEnded(target, true);
        }
      });

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.HOURS);
  }

  @Test
  public void testAvoidFailingTargets() {
    final List<String> targets = asList("t1", "t2");

    provider.targetDispatched("t1");
    provider.targetDispatched("t1");

    provider.targetDispatched("t2");
    provider.targetDispatched("t2");
    provider.targetDispatchEnded("t2", false);

    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, targets));
    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, targets));
  }

  @Test
  public void testProvideFailingTargetsWithoutPendingRequests() {
    final List<String> targets = asList("t1", "t2");

    provider.targetDispatched("t1");
    provider.targetDispatched("t1");

    provider.targetDispatched("t2");
    provider.targetDispatchEnded("t2", false);

    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, targets));
    assertEquals(singletonList("t2"), provider.provideTargetsImpl(1, targets));
  }

  @Test
  public void testClearFailuresOnSuccessfulDispatch() {
    final List<String> targets = asList("t1", "t2");

    provider.targetDispatched("t2");
    provider.targetDispatched("t2");
    provider.targetDispatched("t2");
    provider.targetDispatchEnded("t2", false);

    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, targets));
    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, targets));

    provider.targetDispatchEnded("t2", true);

    provider.targetDispatched("t1");
    provider.targetDispatched("t1");

    assertEquals(singletonList("t2"), provider.provideTargetsImpl(1, targets));
  }

  @Test
  public void testReturnByNumberOfPendingRequests() {
    final List<String> targets = asList("t1", "t2");
    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, targets));
    provider.targetDispatched("t1");
    assertEquals(singletonList("t2"), provider.provideTargetsImpl(1, targets));
    assertEquals(singletonList("t2"), provider.provideTargetsImpl(1, targets));
  }

  @Test
  public void testReturnTheSameTargetMultipleTimes() {
    final List<String> targets = asList("t1", "t2");
    assertEquals(asList("t1", "t2", "t1"), provider.provideTargetsImpl(3, targets));
  }

  @Test
  public void testClearFailuresOnTargetsChange() {
    final List<String> targets = asList("t1", "t2");
    provider.targetDispatchEnded("t1", false);
    provider.notifyTargetsChanged(Collections.singletonList("t2"));
    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, targets));
  }

  @Test
  public void testRoundRobin() {
    final List<String> targets = asList("t1", "t2");

    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, targets));
    assertEquals(singletonList("t2"), provider.provideTargetsImpl(1, targets));

    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, targets));
    assertEquals(singletonList("t2"), provider.provideTargetsImpl(1, targets));
  }
}