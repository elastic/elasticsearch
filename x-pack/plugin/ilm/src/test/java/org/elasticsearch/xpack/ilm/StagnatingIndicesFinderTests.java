/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.MAX_RETRIES_PER_STEP_SETTING;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.MAX_TIME_ON_ACTION_SETTING;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.MAX_TIME_ON_STEP_SETTING;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.isStagnated;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StagnatingIndicesFinderTests extends ESTestCase {

    public void testStagnatingIndicesFinder() {
        var maxTimeOnAction = randomTimeValueInDays();
        var maxTimeOnStep = randomTimeValueInDays();
        Long maxRetriesPerStep = randomLongBetween(2, 100);
        var idxMd1 = randomIndexMetadata();
        var idxMd2 = randomIndexMetadata();
        var idxMd3 = randomIndexMetadata();
        var stagnatingIndices = List.of(idxMd1.indexName, idxMd3.indexName);
        var mockedTimeSupplier = mock(LongSupplier.class);
        var instant = (long) randomIntBetween(100000, 200000);
        var ruleCreator = Stream.<IlmHealthIndicatorService.RuleConfig>of(
            (now, indexMetadata) -> now == instant && stagnatingIndices.contains(indexMetadata.getIndex().getName())
        ).map(rc -> (IlmHealthIndicatorService.RuleCreator) (expectedMaxTimeOnAction, expectedMaxTimeOnStep, expectedMaxRetriesPerStep) -> {
            assertEquals(expectedMaxTimeOnAction, maxTimeOnAction);
            assertEquals(expectedMaxTimeOnStep, maxTimeOnStep);
            assertEquals(expectedMaxRetriesPerStep, maxRetriesPerStep);
            return rc;
        }).collect(Collectors.toList());
        // Per the evaluator, the timeSupplier _must_ be called only twice
        when(mockedTimeSupplier.getAsLong()).thenReturn(instant, instant);

        var stagnatedIdx1 = indexMetadataFrom(idxMd1);
        var stagnatedIdx3 = indexMetadataFrom(idxMd3);
        var finder = createStagnatingIndicesFinder(
            ruleCreator,
            maxTimeOnAction,
            maxTimeOnStep,
            maxRetriesPerStep,
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

    public void testRecreateRules() {
        var maxTimeOnAction = randomTimeValueInDays();
        var maxTimeOnStep = randomTimeValueInDays();
        long maxRetriesPerStep = randomLongBetween(2, 100);
        var accumulator = new ArrayList<Integer>();
        var numberOfRuleConfigs = randomIntBetween(3, 20);

        var ruleCreators = IntStream.range(0, numberOfRuleConfigs)
            .mapToObj(
                i -> (IlmHealthIndicatorService.RuleCreator) (
                    expectedMaxTimeOnAction,
                    expectedMaxTimeOnStep,
                    expectedMaxRetriesPerStep) -> {
                    accumulator.add(i);
                    return new RuleConfigHolder(expectedMaxTimeOnAction, expectedMaxTimeOnStep, expectedMaxRetriesPerStep);
                }
            )
            .toList();

        var finder = createStagnatingIndicesFinder(
            ruleCreators,
            maxTimeOnAction,
            maxTimeOnStep,
            maxRetriesPerStep,
            mock(LongSupplier.class)
        );

        // Test: safety-net ensuring that the settings are gathered when the object is created
        assertEquals(accumulator, IntStream.range(0, numberOfRuleConfigs).boxed().toList());

        var rules = finder.rules();
        assertThat(rules, hasSize(numberOfRuleConfigs));
        // all the rules will have the same value, so it's enough checking the first one
        assertEquals(rules.iterator().next(), new RuleConfigHolder(maxTimeOnAction, maxTimeOnStep, maxRetriesPerStep));

        accumulator.clear();
        maxTimeOnAction = randomTimeValueInDays();
        maxTimeOnStep = randomTimeValueInDays();
        maxRetriesPerStep = randomLongBetween(2, 100);

        // Test: the method `recreateRules` works as expected
        finder.recreateRules(
            Settings.builder()
                .put(MAX_TIME_ON_ACTION_SETTING.getKey(), maxTimeOnAction)
                .put(MAX_TIME_ON_STEP_SETTING.getKey(), maxTimeOnStep)
                .put(MAX_RETRIES_PER_STEP_SETTING.getKey(), maxRetriesPerStep)
                .build()
        );

        var newRules = finder.rules();
        assertThat(rules, hasSize(numberOfRuleConfigs));
        assertNotSame(rules, newRules);
        // all the rules will have the same value, so it's enough checking the first one
        assertEquals(newRules.iterator().next(), new RuleConfigHolder(maxTimeOnAction, maxTimeOnStep, maxRetriesPerStep));
        assertEquals(accumulator, IntStream.range(0, numberOfRuleConfigs).boxed().toList());

        accumulator.clear();
        rules = finder.rules();
        maxTimeOnAction = randomTimeValueInDays();
        maxTimeOnStep = randomTimeValueInDays();
        maxRetriesPerStep = randomLongBetween(2, 100);

        // Test: Force a settings update, ensuring that the method `recreateRules` is called
        finder.clusterService()
            .getClusterSettings()
            .applySettings(
                Settings.builder()
                    .put(MAX_TIME_ON_ACTION_SETTING.getKey(), maxTimeOnAction)
                    .put(MAX_TIME_ON_STEP_SETTING.getKey(), maxTimeOnStep)
                    .put(MAX_RETRIES_PER_STEP_SETTING.getKey(), maxRetriesPerStep)
                    .build()
            );

        newRules = finder.rules();
        assertThat(rules, hasSize(numberOfRuleConfigs));
        assertNotSame(rules, newRules);
        // all the rules will have the same value, so it's enough checking the first one
        assertEquals(newRules.iterator().next(), new RuleConfigHolder(maxTimeOnAction, maxTimeOnStep, maxRetriesPerStep));
        assertEquals(accumulator, IntStream.range(0, numberOfRuleConfigs).boxed().toList());
    }

    record RuleConfigHolder(TimeValue maxTimeOnAction, TimeValue maxTimeOnStep, Long maxRetries)
        implements
            IlmHealthIndicatorService.RuleConfig {
        @Override
        public boolean test(Long now, IndexMetadata indexMetadata) {
            return false;
        }
    }

    public void testStagnatingIndicesEvaluator() {
        var idxMd1 = randomIndexMetadata();
        var indexMetadata = indexMetadataFrom(idxMd1);
        Long moment = 111333111222L;
        {
            // no rule matches
            var executions = randomIntBetween(3, 200);
            var calls = new AtomicInteger(0);
            var rules = IntStream.range(0, executions).mapToObj(i -> (IlmHealthIndicatorService.RuleConfig) (now, idxMd) -> {
                assertEquals(now, moment);
                assertSame(idxMd, indexMetadata);
                calls.incrementAndGet();
                return false;
            }).toList();
            assertFalse(isStagnated(rules, moment, indexMetadata));
            assertEquals(calls.get(), executions);
        }
        {
            var calls = new AtomicReference<>(new ArrayList<Integer>());
            var rules = List.<IlmHealthIndicatorService.RuleConfig>of((now, idxMd) -> { // will be called
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

            assertTrue(isStagnated(rules, moment, indexMetadata));
            assertEquals(calls.get(), List.of(1, 2));
        }
    }

    private static TimeValue randomTimeValueInDays() {
        return TimeValue.parseTimeValue(randomTimeValue(1, 1000, "d"), "some.name");
    }

    private static IndexMetadata indexMetadataUnmanaged(String indexName) {
        return indexMetadataFrom(new IndexMetadataTestCase(indexName, null, null));
    }

    private static IndexMetadata indexMetadataFrom(IndexMetadataTestCase indexMetadataTestCase) {
        var settings = settings(IndexVersion.current());
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
        Collection<IlmHealthIndicatorService.RuleCreator> ruleCreator,
        TimeValue maxTimeOnAction,
        TimeValue maxTimeOnStep,
        long maxRetriesPerStep,
        LongSupplier timeSupplier,
        IndexMetadata... indicesMetadata
    ) {
        var clusterService = mock(ClusterService.class);
        var state = mock(ClusterState.class);
        var metadataBuilder = Metadata.builder();

        Arrays.stream(indicesMetadata).forEach(im -> metadataBuilder.put(im, false));
        when(state.metadata()).thenReturn(metadataBuilder.build());

        when(clusterService.state()).thenReturn(state);
        var settings = Settings.builder()
            .put(MAX_TIME_ON_ACTION_SETTING.getKey(), maxTimeOnAction)
            .put(MAX_TIME_ON_STEP_SETTING.getKey(), maxTimeOnStep)
            .put(MAX_RETRIES_PER_STEP_SETTING.getKey(), maxRetriesPerStep)
            .build();
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Set.of(
                    IlmHealthIndicatorService.MAX_TIME_ON_ACTION_SETTING,
                    IlmHealthIndicatorService.MAX_TIME_ON_STEP_SETTING,
                    IlmHealthIndicatorService.MAX_RETRIES_PER_STEP_SETTING
                )
            )
        );

        return new IlmHealthIndicatorService.StagnatingIndicesFinder(clusterService, ruleCreator, timeSupplier);
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
