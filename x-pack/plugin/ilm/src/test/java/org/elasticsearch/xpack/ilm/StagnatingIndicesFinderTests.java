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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
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
        var ruleEvaluator = new IlmHealthIndicatorService.StagnatingIndicesRuleEvaluator(
            List.of(state -> (stagnatingIndices.contains(state.indexName())))
        );
        var instant = (long) randomIntBetween(100000, 200000);
        // Per the evaluator, the timeSupplier _must_ be called only twice
        when(mockedTimeSupplier.getAsLong()).thenReturn(instant, instant);
        var finder = createStagnatingIndicesFinder(
            ruleEvaluator,
            mockedTimeSupplier,
            idxMetadataUnmanaged(randomAlphaOfLength(10)),                     // non-managed by ILM
            idxMetadata(idxMd1.indexName, idxMd1.policyName, idxMd1.ilmState), // should be stagnated
            idxMetadata(idxMd2.indexName, idxMd2.policyName, idxMd2.ilmState), // won't be stagnated
            idxMetadata(idxMd3.indexName, idxMd3.policyName, idxMd3.ilmState), // should be stagnated
            idxMetadataUnmanaged(randomAlphaOfLength(10))                      // non-managed by ILM
        );

        var foundIndices = finder.find();

        assertThat(foundIndices, hasSize(2));
        assertThat(
            foundIndices,
            containsInAnyOrder(
                new IlmHealthIndicatorService.IndexIlmState(
                    idxMd1.indexName,
                    idxMd1.policyName,
                    idxMd1.ilmState.phase(),
                    idxMd1.ilmState.action(),
                    TimeValue.timeValueMillis(instant - idxMd1.ilmState.actionTime()),
                    idxMd1.ilmState.step(),
                    TimeValue.timeValueMillis(instant - idxMd1.ilmState.stepTime()),
                    idxMd1.ilmState.failedStepRetryCount()
                ),
                new IlmHealthIndicatorService.IndexIlmState(
                    idxMd3.indexName,
                    idxMd3.policyName,
                    idxMd3.ilmState.phase(),
                    idxMd3.ilmState.action(),
                    TimeValue.timeValueMillis(instant - idxMd3.ilmState.actionTime()),
                    idxMd3.ilmState.step(),
                    TimeValue.timeValueMillis(instant - idxMd3.ilmState.stepTime()),
                    idxMd3.ilmState.failedStepRetryCount()
                )
            )
        );
    }

    public void testStagnatingIndicesEvaluator() {
        var idxIlmState = new IlmHealthIndicatorService.IndexIlmState(
            "some-index",
            "some-policy",
            "some-phase",
            "some-action",
            TimeValue.ZERO,
            "some-step",
            TimeValue.ZERO,
            100
        );
        {
            // no rule matches
            var executions = randomIntBetween(3, 200);
            var calls = new AtomicInteger(0);
            var predicates = IntStream.range(0, executions).mapToObj(i -> (Predicate<IlmHealthIndicatorService.IndexIlmState>) a -> {
                assertSame(a, idxIlmState);
                calls.incrementAndGet();
                return false;
            }).toList();
            assertFalse(new IlmHealthIndicatorService.StagnatingIndicesRuleEvaluator(predicates).isStagnated(idxIlmState));
            assertEquals(calls.get(), executions);
        }
        {
            var calls = new AtomicReference<>(new ArrayList<Integer>());
            var predicates = List.<Predicate<IlmHealthIndicatorService.IndexIlmState>>of(a -> { // will be called
                assertSame(a, idxIlmState);
                calls.get().add(1);
                return false;
            }, a -> { // will be called and cut the execution
                assertSame(a, idxIlmState);
                calls.get().add(2);
                return true;
            }, a -> { // won't be called
                assertSame(a, idxIlmState);
                calls.get().add(3);
                return true;
            }, a -> { // won't be called
                assertSame(a, idxIlmState);
                calls.get().add(4);
                return false;
            });

            assertTrue(new IlmHealthIndicatorService.StagnatingIndicesRuleEvaluator(predicates).isStagnated(idxIlmState));
            assertEquals(calls.get(), List.of(1, 2));
        }
    }

    private static IndexMetadata idxMetadataUnmanaged(String indexName) {
        return idxMetadata(indexName, null, null);
    }

    private static IndexMetadata idxMetadata(String indexName, String policyName, LifecycleExecutionState ilmState) {
        var settings = settings(Version.CURRENT);
        var indexMetadataBuilder = IndexMetadata.builder(indexName);

        if (ilmState != null) {
            settings.put(LifecycleSettings.LIFECYCLE_NAME, policyName);
            indexMetadataBuilder.putCustom(ILM_CUSTOM_METADATA_KEY, ilmState.asMap());
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
