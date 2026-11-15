/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;

/** Exercises physical event identity and request filtering across actual data-stream generations. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceDataStreamIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private final List<String> streams = new ArrayList<>();
    private final List<String> templates = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DataStreamsPlugin.class);
    }

    /** Reused IDs in different backing indices must retain their own physical payload and source index. */
    public void testMatchesAcrossBackingIndicesWithRepeatedDocumentIds() throws Exception {
        String stream = "sequence-datastream-events";
        createStream(stream);
        String first = backingIndices(stream).getFirst();
        indexEvent(stream, "shared-start", "old", "A", "old-A", 0);
        String second = rollover(stream);
        indexEvent(stream, "shared-end", "old", "B", "old-B", 1);
        indexEvent(stream, "shared-start", "new", "A", "new-A", 10);
        String third = rollover(stream);
        indexEvent(stream, "shared-end", "new", "B", "new-B", 11);
        assertEquals(List.of(first, second, third), backingIndices(stream));
        refresh(stream);

        for (int primary : List.of(1, 2)) {
            try (var response = run(query(stream, primary))) {
                assertValues(
                    response.values(),
                    List.of(
                        expectedRow("new", "new-A", second, "new-B", third, primary),
                        expectedRow("old", "old-A", first, "old-B", second, primary)
                    )
                );
            }
        }
    }

    /** A request time filter must keep a recent cross-generation pair while excluding an older complete pair. */
    public void testRequestTimeFilterAcrossThirtyBackingIndices() throws Exception {
        String stream = "sequence-thirty-backing-events";
        createStream(stream);
        String oldStart = backingIndices(stream).getFirst();
        indexEvent(stream, "shared-start", "old", "A", "old-A", 0);
        String oldEnd = rollover(stream);
        indexEvent(stream, "shared-end", "old", "B", "old-B", 1);
        for (int generation = 3; generation <= 28; generation++) {
            rollover(stream);
            indexEvent(stream, "noise", "noise-" + generation, "noise", "noise", 2);
        }
        String recentStart = rollover(stream);
        indexEvent(stream, "shared-start", "recent", "A", "recent-A", 10);
        String recentEnd = rollover(stream);
        indexEvent(stream, "shared-end", "recent", "B", "recent-B", 11);
        assertEquals(30, backingIndices(stream).size());
        refresh(stream);

        try (var response = run("FROM " + stream + " | STATS count=COUNT(*)")) {
            assertValues(response.values(), List.of(List.of(30L)));
        }
        for (int primary : List.of(1, 2)) {
            try (var response = run(query(stream, primary))) {
                assertValues(
                    response.values(),
                    List.of(
                        expectedRow("old", "old-A", oldStart, "old-B", oldEnd, primary),
                        expectedRow("recent", "recent-A", recentStart, "recent-B", recentEnd, primary)
                    )
                );
            }
            try (
                var response = run(
                    syncEsqlQueryRequest(query(stream, primary)).pragmas(getPragmas())
                        .filter(rangeQuery("@timestamp").gte(BASE.plusSeconds(9).toString()).lt(BASE.plusSeconds(12).toString()))
                )
            ) {
                assertValues(response.values(), List.of(expectedRow("recent", "recent-A", recentStart, "recent-B", recentEnd, primary)));
            }
            try (
                var response = run(
                    syncEsqlQueryRequest(query(stream, primary)).pragmas(getPragmas())
                        .filter(rangeQuery("@timestamp").gte(BASE.plusSeconds(20).toString()).lt(BASE.plusSeconds(21).toString()))
                )
            ) {
                assertFalse(response.values().hasNext());
            }
        }
    }

    private static String query(String stream, int primary) {
        int member = primary == 1 ? 2 : 1;
        return """
            SEQUENCE BY key WITHIN 30s PRIMARY %s
            (FROM %s METADATA _id,_index | WHERE phase=="A"),
            (FROM %s METADATA _id,_index | WHERE phase=="B")
            | EVAL other_payload=MEMBER(%s,payload), other_id=MEMBER(%s,_id), other_index=MEMBER(%s,_index)
            | KEEP key,payload,_id,_index,other_payload,other_id,other_index | SORT key | LIMIT 10
            """.formatted(primary, stream, stream, member, member, member);
    }

    private static List<Object> expectedRow(
        String key,
        String startPayload,
        String startIndex,
        String endPayload,
        String endIndex,
        int primary
    ) {
        return primary == 1
            ? List.of(key, startPayload, "shared-start", startIndex, endPayload, "shared-end", endIndex)
            : List.of(key, endPayload, "shared-end", endIndex, startPayload, "shared-start", startIndex);
    }

    private void createStream(String stream) throws Exception {
        String template = stream + "-template";
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request(template).indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(stream))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .template(
                            Template.builder()
                                .settings(
                                    Settings.builder()
                                        .put("index.mode", "standard")
                                        .put("index.number_of_shards", 2)
                                        .put("index.number_of_replicas", 0)
                                        .put("index.routing.allocation.total_shards_per_node", 1)
                                )
                                .mappings(CompressedXContent.fromJSON("""
                                    {"dynamic":"strict","properties":{
                                      "@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},
                                      "phase":{"type":"keyword"},"payload":{"type":"keyword"}}}
                                    """))
                        )
                        .build()
                )
            )
        );
        templates.add(template);
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, stream)
            )
        );
        streams.add(stream);
        ensureGreen(stream);
    }

    private String rollover(String stream) {
        var response = indicesAdmin().prepareRolloverIndex(stream).get();
        assertTrue(response.isRolledOver());
        assertTrue(response.isShardsAcknowledged());
        ensureGreen(stream);
        return response.getNewIndex();
    }

    private List<String> backingIndices(String stream) {
        return client().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { stream })
        ).actionGet().getDataStreams().getFirst().getDataStream().getIndices().stream().map(index -> index.getName()).toList();
    }

    private void indexEvent(String stream, String id, String key, String phase, String payload, int seconds) {
        prepareIndex(stream).setOpType(DocWriteRequest.OpType.CREATE)
            .setId(id)
            .setSource("@timestamp", BASE.plusSeconds(seconds).toString(), "key", key, "phase", phase, "payload", payload)
            .get();
    }

    /** Check native resources before deleting backing indices, since deletion could hide retained PIT readers. */
    @After
    public void releaseStreamFixtures() throws Exception {
        try {
            assertBusy(() -> {
                for (SearchService search : internalCluster().getInstances(SearchService.class)) {
                    assertEquals(0, search.getActiveContexts());
                }
                assertThat(
                    clusterAdmin().prepareListTasks()
                        .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME, TransportSearchAction.TYPE.name() + "*")
                        .get()
                        .getTasks(),
                    empty()
                );
            });
            ensureBlocksReleased();
        } finally {
            for (String stream : streams) {
                assertAcked(
                    client().execute(
                        DeleteDataStreamAction.INSTANCE,
                        new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { stream })
                    )
                );
            }
            for (String template : templates) {
                assertAcked(
                    client().execute(
                        TransportDeleteComposableIndexTemplateAction.TYPE,
                        new TransportDeleteComposableIndexTemplateAction.Request(template)
                    )
                );
            }
        }
    }
}
