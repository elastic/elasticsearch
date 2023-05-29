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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
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
        var ruleEvaluator = new IlmHealthIndicatorService.StagnatingIndicesRuleEvaluator(
            List.of((now, indexMetadata) -> now == instant && stagnatingIndices.contains(indexMetadata.getIndex().getName()))
        );
        // Per the evaluator, the timeSupplier _must_ be called only twice
        when(mockedTimeSupplier.getAsLong()).thenReturn(instant, instant);

        var stagnatedIdx1 = idxMetadataFrom(idxMd1);
        var stagnatedIdx2 = idxMetadataFrom(idxMd3);
        var finder = createStagnatingIndicesFinder(
            ruleEvaluator,
            mockedTimeSupplier,
            idxMetadataUnmanaged(randomAlphaOfLength(10)), // non-managed by ILM
            stagnatedIdx1,                                 // should be stagnated
            idxMetadataFrom(idxMd2),                       // won't be stagnated
            stagnatedIdx2,                                 // should be stagnated
            idxMetadataUnmanaged(randomAlphaOfLength(10))  // non-managed by ILM
        );

        var foundIndices = finder.find();

        assertThat(foundIndices, hasSize(2));
        assertThat(foundIndices, containsInAnyOrder(stagnatedIdx1, stagnatedIdx2));
    }

    public void testStagnatingIndicesEvaluator() {
        var idxMd1 = randomIndexMetadata();
        var indexMetadata = idxMetadataFrom(idxMd1);
        Long moment = 111333111222L;
        {
            // no rule matches
            var executions = randomIntBetween(3, 200);
            var calls = new AtomicInteger(0);
            var predicates = IntStream.range(0, executions).mapToObj(i -> (BiPredicate<Long, IndexMetadata>) (now, idxMd) -> {
                assertEquals(now, moment);
                assertSame(idxMd, indexMetadata);
                calls.incrementAndGet();
                return false;
            }).toList();
            assertFalse(new IlmHealthIndicatorService.StagnatingIndicesRuleEvaluator(predicates).isStagnated(moment, indexMetadata));
            assertEquals(calls.get(), executions);
        }
        {
            var calls = new AtomicReference<>(new ArrayList<Integer>());
            var predicates = List.<BiPredicate<Long, IndexMetadata>>of((now, idxMd) -> { // will be called
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
            });

            assertTrue(new IlmHealthIndicatorService.StagnatingIndicesRuleEvaluator(predicates).isStagnated(moment, indexMetadata));
            assertEquals(calls.get(), List.of(1, 2));
        }
    }

    private static IndexMetadata idxMetadataUnmanaged(String indexName) {
        return idxMetadataFrom(new IndexMetadataTestCase(indexName, null, null));
    }

    private static IndexMetadata idxMetadataFrom(IndexMetadataTestCase indexMetadataTestCase) {
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
        IlmHealthIndicatorService.StagnatingIndicesRuleEvaluator evaluator,
        LongSupplier timeSupplier,
        IndexMetadata... indicesMetadata
    ) {
        var clusterService = mock(ClusterService.class);
        var state = mock(ClusterState.class);
        var metadataBuilder = Metadata.builder();

        Arrays.stream(indicesMetadata).forEach(im -> metadataBuilder.put(im, true));
        when(state.metadata()).thenReturn(metadataBuilder.build());

        when(clusterService.state()).thenReturn(state);

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
