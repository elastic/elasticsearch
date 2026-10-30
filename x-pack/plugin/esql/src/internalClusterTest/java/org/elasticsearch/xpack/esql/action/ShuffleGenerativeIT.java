/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.generator.ShuffleQueryGenerator;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/** One-to-one source keys bound intermediate row counts in deep trees without imposing limits inside subqueries. */
@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 1)
public class ShuffleGenerativeIT extends AbstractEsqlIntegTestCase {
    private static final List<String> INDICES = List.of("shuffle_events", "shuffle_int", "shuffle_long");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    public void testGeneratedPlanShapes() throws Exception {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        assumeTrue("subquery in FROM required", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        for (String index : INDICES) {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .setMapping(
                        "emp_no",
                        index.equals("shuffle_int") ? "type=integer" : "type=long",
                        "languages",
                        "type=integer",
                        "first_name",
                        "type=keyword",
                        "salary",
                        "type=long"
                    )
            );
            BulkRequestBuilder bulk = client().prepareBulk(index);
            for (int row = 0; row < 12; row++) {
                bulk.add(
                    client().prepareIndex()
                        .setSource(
                            Map.of("emp_no", row, "languages", row + 100, "first_name", "key-" + row, "salary", randomIntBetween(1, 9))
                        )
                );
            }
            bulk.add(client().prepareIndex().setSource(Map.of("languages", 112, "first_name", "missing-key", "salary", 1)));
            assertFalse(bulk.get().hasFailures());
        }
        client().admin().indices().prepareRefresh(INDICES.toArray(String[]::new)).get();

        ShuffleQueryGenerator generator = new ShuffleQueryGenerator(random(), INDICES);
        for (int joins : new int[] { 1, 2, 5, 10, 20 }) {
            String query = generator.query(joins, joins == 20 ? 4 : randomIntBetween(0, 4));
            try (EsqlQueryResponse baseline = run(syncEsqlQueryRequest(query).acceptedPragmaRisks(true).pragmas(QueryPragmas.EMPTY))) {
                List<List<Object>> expected = getValuesList(baseline);
                assertThat("query must not truncate its result:\n" + query, expected.size(), lessThan(10_000));
                for (String strategy : List.of("partitioned", "broadcast")) {
                    for (boolean shuffleAgg : List.of(false, true)) {
                        QueryPragmas pragmas = new QueryPragmas(
                            Settings.builder()
                                .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), randomIntBetween(1, 3))
                                .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
                                .put(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), strategy)
                                .put(QueryPragmas.SHUFFLE_AGG.getKey(), shuffleAgg)
                                .put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), randomIntBetween(1, 3))
                                .put(QueryPragmas.TASK_CONCURRENCY.getKey(), randomIntBetween(1, 3))
                                .build()
                        );
                        String context = "pragmas=" + pragmas.getSettings() + "\n" + query;
                        try (
                            EsqlQueryResponse shuffled = run(
                                syncEsqlQueryRequest(query).acceptedPragmaRisks(true).pragmas(pragmas).profile(true)
                            )
                        ) {
                            assertThat(context, shuffled.columns(), equalTo(baseline.columns()));
                            assertThat(context, getValuesList(shuffled), equalTo(expected));
                            assertTrue(
                                "query must exercise staged execution:\n" + context,
                                shuffled.profile().plans().stream().anyMatch(plan -> plan.planTree().contains("StageSinkExec"))
                            );
                        } catch (Exception | AssertionError e) {
                            throw new AssertionError(context, e);
                        }
                    }
                }
            } catch (Exception | AssertionError e) {
                throw new AssertionError("generated query:\n" + query, e);
            }
        }
    }
}
