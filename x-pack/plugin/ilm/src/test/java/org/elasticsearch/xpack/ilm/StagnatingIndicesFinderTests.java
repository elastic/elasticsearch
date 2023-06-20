/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.isStagnated;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StagnatingIndicesFinderTests extends ESTestCase {

    public void testStagnatingIndicesFinder() {
        var idxMd1 = randomIndexMetadata();
        var idxMd2 = randomIndexMetadata();
        var idxMd3 = randomIndexMetadata();
        var stagnatingIndices = List.of(idxMd1.indexName, idxMd3.indexName);
        var mockedTimeSupplier = mock(LongSupplier.class);
        var instant = (long) randomIntBetween(100000, 200000);
        var ruleEvaluator = Stream.<IlmHealthIndicatorService.RuleConfig>of(
            (now, indexMetadata) -> now == instant && stagnatingIndices.contains(indexMetadata.getIndex().getName())
        ).map(rc -> (IlmHealthIndicatorService.RuleCreator) (a, b, c) -> {
            assertEquals(a, TimeValue.timeValueDays(1));
            assertEquals(b, TimeValue.timeValueDays(1));
            assertEquals(c, Long.valueOf(100));
            return rc;
        }).collect(Collectors.toList());
        // Per the evaluator, the timeSupplier _must_ be called only twice
        when(mockedTimeSupplier.getAsLong()).thenReturn(instant, instant);

        var stagnatedIdx1 = indexMetadataFrom(idxMd1);
        var stagnatedIdx3 = indexMetadataFrom(idxMd3);
        var finder = createStagnatingIndicesFinder(
            ruleEvaluator,
            mockedTimeSupplier,
            indexMetadataUnmanaged(randomAlphaOfLength(10)), // non-managed by ILM
            stagnatedIdx1,                                 // should be stagnated
            indexMetadataFrom(idxMd2),                       // won't be stagnated
            stagnatedIdx3,                                 // should be stagnated
            indexMetadataUnmanaged(randomAlphaOfLength(10))  // non-managed by ILM
        );

        var foundIndices = finder.find();

        assertThat(foundIndices, hasSize(2));
        assertThat(foundIndices, containsInAnyOrder(stagnatedIdx1, stagnatedIdx3));
    }

    public void testStagnatingIndicesEvaluator() {
        var idxMd1 = randomIndexMetadata();
        var indexMetadata = indexMetadataFrom(idxMd1);
        Long moment = 111333111222L;
        {
            // no rule matches
            var executions = randomIntBetween(3, 200);
            var calls = new AtomicInteger(0);
            var defaultMaxTimeOnAction = randTimeValue();
            var defaultMaxTimeOnStep = randTimeValue();
            Long defaultMaxRetriesPerStep = randomLongBetween(2, 100);
            var rules = IntStream.range(0, executions).mapToObj(i -> (IlmHealthIndicatorService.RuleConfig) (now, idxMd) -> {
                assertEquals(now, moment);
                assertSame(idxMd, indexMetadata);
                calls.incrementAndGet();
                return false;
            }).map(rc -> (IlmHealthIndicatorService.RuleCreator) (a, b, c) -> {
                assertEquals(defaultMaxTimeOnAction, a);
                assertEquals(defaultMaxTimeOnStep, b);
                assertEquals(defaultMaxRetriesPerStep, c);
                return rc;
            }).toList();
            assertFalse(isStagnated(rules, defaultMaxTimeOnAction, defaultMaxTimeOnStep, defaultMaxRetriesPerStep, moment, indexMetadata));
            assertEquals(calls.get(), executions);
        }
        {
            var defaultMaxTimeOnAction = randTimeValue();
            var defaultMaxTimeOnStep = randTimeValue();
            Long defaultMaxRetriesPerStep = randomLongBetween(2, 100);
            var calls = new AtomicReference<>(new ArrayList<Integer>());
            var rules = Stream.<IlmHealthIndicatorService.RuleConfig>of((now, idxMd) -> { // will be called
                assertEquals(now, moment);
                assertSame(idxMd, indexMetadata);
                calls.get().add(1);
                return false;
            }, (now, idxMd) -> { // will be called and cut the execution
                assertEquals(now, moment);
                assertSame(idxMd, indexMetadata);
                calls.get().add(2);
                return true;
            }, (now, idxMd) -> { // won't be called
                assertEquals(now, moment);
                assertSame(idxMd, indexMetadata);
                calls.get().add(3);
                return true;
            }, (now, idxMd) -> { // won't be called
                assertEquals(now, moment);
                assertSame(idxMd, indexMetadata);
                calls.get().add(4);
                return false;
            }).map(rc -> (IlmHealthIndicatorService.RuleCreator) (a, b, c) -> {
                assertEquals(defaultMaxTimeOnAction, a);
                assertEquals(defaultMaxTimeOnStep, b);
                assertEquals(defaultMaxRetriesPerStep, c);
                return rc;
            }).toList();

            assertTrue(isStagnated(rules, defaultMaxTimeOnAction, defaultMaxTimeOnStep, defaultMaxRetriesPerStep, moment, indexMetadata));
            assertEquals(calls.get(), List.of(1, 2));
        }
    }

    private static TimeValue randTimeValue() {
        return TimeValue.parseTimeValue(randomTimeValue(), "some.name");
    }

    private static IndexMetadata indexMetadataUnmanaged(String indexName) {
        return indexMetadataFrom(new IndexMetadataTestCase(indexName, null, null));
    }

    private static IndexMetadata indexMetadataFrom(IndexMetadataTestCase indexMetadataTestCase) {
        var settings = settings(Version.CURRENT);
        var indexMetadataBuilder = IndexMetadata.builder(indexMetadataTestCase.indexName);

        if (indexMetadataTestCase.ilmState != null) {
            settings.put(LifecycleSettings.LIFECYCLE_NAME, indexMetadataTestCase.policyName);
            indexMetadataBuilder.putCustom(ILM_CUSTOM_METADATA_KEY, indexMetadataTestCase.ilmState.asMap());
        }

        return indexMetadataBuilder.settings(settings)
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
    }

    private IlmHealthIndicatorService.StagnatingIndicesFinder createStagnatingIndicesFinder(
        Collection<IlmHealthIndicatorService.RuleCreator> evaluator,
        LongSupplier timeSupplier,
        IndexMetadata... indicesMetadata
    ) {
        var clusterService = mock(ClusterService.class);
        var state = mock(ClusterState.class);
        var metadataBuilder = Metadata.builder();

        Arrays.stream(indicesMetadata).forEach(im -> metadataBuilder.put(im, false));
        when(state.metadata()).thenReturn(metadataBuilder.build());

        when(clusterService.state()).thenReturn(state);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                Settings.EMPTY,
                new HashSet<>(
                    List.of(
                        IlmHealthIndicatorService.MAX_TIME_ON_ACTION_SETTING,
                        IlmHealthIndicatorService.MAX_TIME_ON_STEP_SETTING,
                        IlmHealthIndicatorService.MAX_RETRIES_PER_STEP_SETTING
                    )
                )
            )
        );

        return new IlmHealthIndicatorService.StagnatingIndicesFinder(clusterService, evaluator, timeSupplier);
    }

    static IndexMetadataTestCase randomIndexMetadata() {
        return new IndexMetadataTestCase(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            LifecycleExecutionState.builder()
                .setPhase(randomAlphaOfLength(5))
                .setAction(randomAlphaOfLength(10))
                .setActionTime((long) randomIntBetween(0, 10000))
                .setStep(randomAlphaOfLength(20))
                .setStepTime((long) randomIntBetween(0, 10000))
                .setFailedStepRetryCount(randomIntBetween(0, 1000))
                .build()
        );
    }

    record IndexMetadataTestCase(String indexName, String policyName, LifecycleExecutionState ilmState) {}
}
