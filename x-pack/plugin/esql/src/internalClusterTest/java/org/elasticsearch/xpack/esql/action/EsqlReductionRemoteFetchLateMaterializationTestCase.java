/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies the distributed remote-fetch prototype for the linear TopN family.
 */
public abstract class EsqlReductionRemoteFetchLateMaterializationTestCase extends AbstractEsqlIntegTestCase {
    private final int shardCount;
    private final int maxConcurrentNodes;
    private final int taskConcurrency;

    EsqlReductionRemoteFetchLateMaterializationTestCase(@Name("TestCase") TestCase testCase) {
        this.shardCount = testCase.shardCount;
        this.maxConcurrentNodes = testCase.maxConcurrentNodes;
        this.taskConcurrency = testCase.taskConcurrency;
    }

    @BeforeClass
    public static void checkCapabilities() {
        assumeTrue("pragmas only enabled on snapshot builds", Build.current().isSnapshot());
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
    }

    public record TestCase(int shardCount, int maxConcurrentNodes, int taskConcurrency) {}

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var result = new ArrayList<Object[]>();
        for (int shardCount : new int[] { 1, 5 }) {
            for (int maxConcurrentNodes : new int[] { 1, 5 }) {
                for (int taskConcurrency : new int[] { 1, 5 }) {
                    result.add(new Object[] { new TestCase(shardCount, maxConcurrentNodes, taskConcurrency) });
                }
            }
        }
        return result;
    }

    private void setupIndex() throws Exception {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("properties");
        {
            mapping.startObject("sorted").field("type", "long").endObject();
            mapping.startObject("filtered").field("type", "long").endObject();
            mapping.startObject("read").field("type", "long").endObject();
            mapping.startObject("more").field("type", "long").endObject();
            mapping.startObject("some_more").field("type", "long").endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("test").setSettings(indexSettings(10, 0)).setMapping(mapping.endObject()).get();

        var builders = IntStream.range(0, 1024)
            .mapToObj(
                i -> prepareIndex("test").setId(Integer.toString(i))
                    .setSource("read", i, "sorted", i * 2, "filtered", i * 3, "more", i * 4, "some_more", i * 5)
            )
            .toList();
        indexRandom(true, builders);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), MockSearchService.TestPlugin.class, SpatialPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(PlannerSettings.REDUCTION_LATE_MATERIALIZATION.getKey(), true)
            .put(PlannerSettings.REMOTE_FETCH_LATE_MATERIALIZATION.getKey(), true)
            .build();
    }

    public void testRemoteFetchesDeferredFieldAfterReduceTopN() throws Exception {
        testRemoteFetchAfterReduceTopN(
            "from test | sort sorted desc | limit 3 | keep sorted, read",
            Set.of("sorted"),
            List.of(List.of(2046L, 1023L), List.of(2044L, 1022L), List.of(2042L, 1021L))
        );
    }

    public void testRemoteFetchesMultipleDeferredFieldsAfterReduceTopN() throws Exception {
        testRemoteFetchAfterReduceTopN(
            "from test | sort sorted desc, more asc | limit 3 | keep read, some_more",
            Set.of("sorted", "more"),
            List.of(List.of(1023L, 5115L), List.of(1022L, 5110L), List.of(1021L, 5105L))
        );
    }

    private void testRemoteFetchAfterReduceTopN(
        String query,
        Set<String> expectedDataLoadedFields,
        Iterable<Iterable<Object>> expectedValues
    ) throws Exception {
        setupIndex();
        try (var result = sendQuery(query)) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertValues(result.values(), expectedValues);
            assertSingleKeyFieldExtracted(result, ComputeService.DATA_DESCRIPTION, expectedDataLoadedFields);
            assertNoValuesReaderOperator(result, ComputeService.REDUCE_DESCRIPTION);
            assertNoValuesReaderOperator(result, "final");
            assertOperatorPresent(result, ComputeService.REDUCE_DESCRIPTION, "RemoteFetchHandleOperator");
            assertOperatorPresent(result, "final", "RemoteFetchOperator");
        }
    }

    private static void assertSingleKeyFieldExtracted(EsqlQueryResponse response, String driverName, Set<String> expectedLoadedFields) {
        long totalValuesLoader = 0;
        for (var driverProfile : response.profile().drivers().stream().filter(d -> d.description().equals(driverName)).toList()) {
            OperatorStatus operatorStatus = singleValue(
                Strings.format(
                    "Only a single ValuesSourceReaderOperator should be present in driver '%s'; "
                        + "more than that means we didn't move the operator in the planner correctly",
                    driverName
                ),
                driverProfile.operators().stream().filter(o -> o.operator().startsWith("ValuesSourceReaderOperator")).toList()
            );
            var status = (ValuesSourceReaderOperatorStatus) operatorStatus.status();
            totalValuesLoader += status.valuesLoaded();
            if (status.valuesLoaded() == 0) {
                continue;
            }
            assertThat(status.readersBuilt().size(), equalTo(expectedLoadedFields.size()));
            for (String field : status.readersBuilt().keySet()) {
                assertTrue(
                    "Field " + field + " was not expected to be loaded in driver " + driverName,
                    expectedLoadedFields.stream().anyMatch(field::contains)
                );
            }
        }
        assertThat("Values should have been loaded", totalValuesLoader, greaterThan(0L));
    }

    private static void assertNoValuesReaderOperator(EsqlQueryResponse response, String driverName) {
        long valuesReaders = response.profile()
            .drivers()
            .stream()
            .filter(d -> d.description().equals(driverName))
            .flatMap(driver -> driver.operators().stream())
            .filter(operator -> operator.operator().startsWith("ValuesSourceReaderOperator"))
            .count();
        assertThat("unexpected values reader in driver [" + driverName + "]", valuesReaders, equalTo(0L));
    }

    private static void assertOperatorPresent(EsqlQueryResponse response, String driverName, String operatorPrefix) {
        long matchingOperators = response.profile()
            .drivers()
            .stream()
            .filter(d -> d.description().equals(driverName))
            .flatMap(driver -> driver.operators().stream())
            .filter(operator -> operator.operator().startsWith(operatorPrefix))
            .count();
        assertThat("expected operator [" + operatorPrefix + "] in driver [" + driverName + "]", matchingOperators, greaterThan(0L));
    }

    private EsqlQueryResponse sendQuery(String query) {
        return client().execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest(query).pragmas(
                new QueryPragmas(
                    Settings.builder()
                        .put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), maxConcurrentNodes)
                        .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), shardCount)
                        .put(QueryPragmas.TASK_CONCURRENCY.getKey(), taskConcurrency)
                        .put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), true)
                        .build()
                )
            ).profile(true)
        ).actionGet(1, TimeUnit.MINUTES);
    }
}
