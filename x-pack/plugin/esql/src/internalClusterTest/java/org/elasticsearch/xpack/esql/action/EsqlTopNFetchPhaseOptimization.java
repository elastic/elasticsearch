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
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

// Verifies that the value source reader operator is optimized into the data node instead of the worker node.
@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class EsqlTopNFetchPhaseOptimization extends AbstractEsqlIntegTestCase {
    private static final int SHARD_COUNT = 10;

    @Before
    public void setupIndex() throws IOException {
        assumeTrue("requires query pragmas", canUseQueryPragmas());

        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("properties");
        {
            mapping.startObject("bar").field("type", "keyword").endObject();
            mapping.startObject("foo").field("type", "long").endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("test").setSettings(indexSettings(10, 0)).setMapping(mapping.endObject()).get();

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 1024; i++) {
            bulk.add(prepareIndex("test").setId(Integer.toString(i)).setSource("foo", i, "bar", i + ""));
        }
        bulk.get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockSearchService.TestPlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        Settings.Builder settings = Settings.builder();
        settings.put(super.getPragmas().getSettings());
        settings.put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), true);
        return new QueryPragmas(settings.build());
    }

    // FIXME(gal, NOCOMMIT) rename
    public void testTopNOperatorDoesTheThingy() throws Exception {
        try (var result = sendQuery()) {
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
            // assertThat(block.getLong(0), equalTo(30L));
            System.out.println(result);
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

    private static EsqlQueryResponse sendQuery() {
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(client())
            // Ensures there is no TopN pushdown to lucene, and that the pause happens after the TopN operator has been applied.
            .query("from test | sort foo + 1 | limit 3 | stats sum(length(bar))")
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
