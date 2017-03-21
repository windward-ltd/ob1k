package com.outbrain.ob1k.consul;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by ahadadi on 18/03/2017.
 */
@RunWith(MockitoJUnitRunner.class)
public class CooperativeTargetProviderTest {

  private static final List<String> TARGETS = asList("t1", "t2");
  private static final int PENALTY = 2;

  @Mock
  private HealthyTargetsList healthyTargetsList;

  private CooperativeTargetProvider provider;

  @Before
  public void setup() {
    provider = new CooperativeTargetProvider(healthyTargetsList, "urlSuffix", Collections.singletonMap("tag", 1));
  }

  @Test
  public void testReturnByNumberOfPendingRequests2() {
    provider.targetDispatched("t1");
    final List<String> t = Collections.nCopies(12, "t");
    for (int i=0; i < 10_000_000; i++) {
      provider.provideTargetsImpl(2, t);
    }
  }

  @Test
  public void testReturnByNumberOfPendingRequests() {
    provider.targetDispatched("t1");
    assertEquals(singletonList("t2"), provider.provideTargetsImpl(1, TARGETS));
  }

  @Test
  public void testReturnTheSameTargetMultipleTimes() {
    assertEquals(asList("t1", "t2", "t1"), provider.provideTargetsImpl(3, TARGETS));
  }

  @Test
  public void testClearPenaltiesOnTargetsChange() {
    provider.targetDispatchEnded("t1", false);
    provider.onTargetsChanged(Collections.emptyList());
    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, TARGETS));
  }

  @Test
  public void testRoundRobin() {
    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, TARGETS));
    assertEquals(singletonList("t2"), provider.provideTargetsImpl(1, TARGETS));

    assertEquals(singletonList("t1"), provider.provideTargetsImpl(1, TARGETS));
    assertEquals(singletonList("t2"), provider.provideTargetsImpl(1, TARGETS));
  }
}