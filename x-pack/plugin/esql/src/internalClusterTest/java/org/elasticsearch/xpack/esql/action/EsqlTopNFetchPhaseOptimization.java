/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.LongVectorBlock;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

// Verifies that the value source reader operator is optimized into the data node instead of the worker node.
@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class EsqlTopNFetchPhaseOptimization extends AbstractEsqlIntegTestCase {
    private static final int SHARD_COUNT = 1;

    @Before
    public void setupIndex() throws Exception {
        assumeTrue("requires query pragmas", canUseQueryPragmas());

        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("properties");
        {
            mapping.startObject("sorted").field("type", "long").endObject();
            mapping.startObject("filtered").field("type", "long").endObject();
            mapping.startObject("read").field("type", "long").endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("test").setSettings(indexSettings(10, 0)).setMapping(mapping.endObject()).get();

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 1024; i++) {
            bulk.add(prepareIndex("test").setId(Integer.toString(i)).setSource("read", i, "sorted", i * 2, "filtered", i * 3));
        }
        bulk.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), MockSearchService.TestPlugin.class, SpatialPlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        Settings.Builder settings = Settings.builder();
        settings.put(super.getPragmas().getSettings());
        settings.put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), true);
        return new QueryPragmas(settings.build());
    }

    public void testNoPushdowns() throws Exception {
        testTopNOperatorDoesTheThingy("from test | sort sorted + 1 desc | limit 3 | stats sum(read)");
    }

    public void testPushdownTopN() throws Exception {
        testTopNOperatorDoesTheThingy("from test | sort sorted desc | limit 3 | stats sum(read)");
    }

    private void testTopNOperatorDoesTheThingy(String query) throws Exception {
        try (var result = sendQuery(query)) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            var dataValuesSourceReaderOperatorStatus = getValueSourceReaderOperatorStatus(result, "data");
            assertThat(dataValuesSourceReaderOperatorStatus.readersBuilt().keySet(), hasSize(1));
            var nodeReduceValuesSourceReaderOperatorStatus = getValueSourceReaderOperatorStatus(result, "node_reduce");
            assertThat(nodeReduceValuesSourceReaderOperatorStatus.readersBuilt().keySet(), hasSize(1));
            var page = singleValue(result.pages());
            assertThat(page.getPositionCount(), equalTo(1));
            LongVectorBlock block = page.getBlock(0);
            assertThat(block.getPositionCount(), equalTo(1));
            assertThat(block.getLong(0), equalTo(1021L + 1022 + 1023));
        }
    }

    private static ValuesSourceReaderOperatorStatus getValueSourceReaderOperatorStatus(EsqlQueryResponse response, String driverName) {
        DriverProfile driverProfile = response.profile().drivers().stream().filter(d -> d.description().equals(driverName)).findAny().get();
        OperatorStatus operatorStatus = singleValue(
            Strings.format(
                "Only a single ValuesSourceReaderOperator should be present in driver '%s'; "
                    + "more than that means we didn't move the operator in the planner correctly",
                driverName
            ),
            driverProfile.operators().stream().filter(o -> o.operator().startsWith("ValuesSourceReaderOperator")).toList()
        );
        return (ValuesSourceReaderOperatorStatus) operatorStatus.status();
    }

    private static EsqlQueryResponse sendQuery(String query) {
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(client())
            // Ensures there is no TopN pushdown to lucene, and that the pause happens after the TopN operator has been applied.
            .query(query)
            .pragmas(
                new QueryPragmas(
                    Settings.builder()
                        // Configured to ensure that there is only one worker handling all the shards, so that we can assert the correct
                        // expected behavior.
                        .put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), 3)
                        .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), SHARD_COUNT)
                        .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 3)
                        .put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), true)
                        .build()
                )
            )
            .profile(true)
            .execute()
            .actionGet(1, TimeUnit.MINUTES);
    }
}
