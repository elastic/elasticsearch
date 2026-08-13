/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.ComputeService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Race where a field unmapped at field-caps resolution gets dynamically mapped before data-node execution
 * (https://github.com/elastic/elasticsearch/issues/154011). A compatible raced-in type loads normally; an
 * incompatible one fails with a descriptive error instead of the opaque sanity-check one.
 */
public class MappingUpdateRaceIT extends AbstractEsqlIntegTestCase {

    private static final String FIELD = "features.topic_id";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(MapperExtrasPlugin.class);
        return plugins;
    }

    /** The original reproducer: resolved as {@code long}, raced in as {@code integer}. */
    public void testIntegerMappingAddedBetweenFieldCapsAndExecution() throws Exception {
        setupIndices("long", "integer");
        ElasticsearchStatusException cause = expectMappingRaceFailure("FROM idx_* | STATS s = SUM(features.topic_id)", "idx_dyn");
        assertThat(cause.getMessage(), containsString("field [features.topic_id] was resolved as type [long]"));
        assertThat(cause.getMessage(), containsString("mapped as incompatible type [integer] in index [idx_dyn]"));
    }

    /** Non-numeric raced-in type: resolved as {@code long}, raced in as {@code keyword}. */
    public void testKeywordMappingAddedBetweenFieldCapsAndExecution() throws Exception {
        setupIndices("long", "keyword");
        ElasticsearchStatusException cause = expectMappingRaceFailure("FROM idx_* | STATS s = SUM(features.topic_id)", "idx_dyn");
        assertThat(cause.getMessage(), containsString("field [features.topic_id] was resolved as type [long]"));
        assertThat(cause.getMessage(), containsString("mapped as incompatible type [keyword] in index [idx_dyn]"));
    }

    /** Raced-in type that ES|QL cannot model: the error reports the mapper's type name, not "unsupported". */
    public void testUnsupportedMappingAddedBetweenFieldCapsAndExecution() throws Exception {
        setupIndices("long", "rank_feature");
        ElasticsearchStatusException cause = expectMappingRaceFailure("FROM idx_* | STATS s = SUM(features.topic_id)", "idx_dyn");
        assertThat(cause.getMessage(), containsString("field [features.topic_id] was resolved as type [long]"));
        assertThat(cause.getMessage(), containsString("mapped as incompatible type [rank_feature] in index [idx_dyn]"));
    }

    /** The raced-in mapping matches the resolved type: no error, the raced-in doc's value loads normally. */
    public void testMatchingMappingAddedBetweenFieldCapsAndExecution() throws Exception {
        setupIndices("long", "long");
        try (var resp = runWithMappingRace("FROM idx_* | STATS s = SUM(features.topic_id)", "idx_dyn")) {
            assertThat(EsqlTestUtils.getValuesList(resp).getFirst().getFirst(), equalTo(43L));
        }
    }

    /** Same widened family ({@code integer} vs raced-in {@code short}): both load as INT blocks, no error. */
    public void testSameFamilyMappingAddedBetweenFieldCapsAndExecution() throws Exception {
        setupIndices("integer", "short");
        try (var resp = runWithMappingRace("FROM idx_* | STATS s = SUM(features.topic_id)", "idx_dyn")) {
            assertThat(EsqlTestUtils.getValuesList(resp).getFirst().getFirst(), equalTo(43L));
        }
    }

    /**
     * {@code unmapped_fields="load"} race on a node with mixed shards: the potentially-unmapped marker survives the
     * local optimizer and its shard context uses the raced-in mapping's loader. This path is not covered by the
     * descriptive check yet, so it documents the still-opaque sanity-check error.
     */
    public void testLoadModeMappingAddedBetweenFieldCapsAndExecution() throws Exception {
        // Pin both shards to one data node so its local plan covers both, keeping the potentially-unmapped marker.
        String node = internalCluster().getDataNodeInstance(ClusterService.class).localNode().getName();
        createPlainIndex("idx_plain", node);
        createDynamicIndex("idx_dyn", "integer", node);
        indexInitialDocs(false);

        // KEEP rather than an aggregation: COUNT gets pushed down to Lucene as a doc count, never extracting values.
        Exception failure = expectThrows(
            Exception.class,
            () -> runWithMappingRace("SET unmapped_fields = \"load\"; FROM idx_* | KEEP features.topic_id", "idx_dyn").close()
        );
        IllegalStateException cause = (IllegalStateException) ExceptionsHelper.unwrap(failure, IllegalStateException.class);
        assertNotNull("expected the query to fail with the sanity-check error, got: " + failure, cause);
        assertThat(cause.getMessage(), containsString("NOT IN (NULL, BYTES_REF)"));
    }

    /**
     * {@code unmapped_fields="load"} race where all of the node's shards have the raced-in mapping: the local
     * optimizer promotes the marker to keyword, landing back on the path covered by the descriptive check.
     */
    public void testLoadModePromotedMappingAddedBetweenFieldCapsAndExecution() throws Exception {
        createDynamicIndex("idx_dyn", "integer", null);
        prepareIndex("idx_dyn").setSource("other", "b").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureGreen("idx_dyn");

        ElasticsearchStatusException cause = expectMappingRaceFailure(
            "SET unmapped_fields = \"load\"; FROM idx_dyn | KEEP features.topic_id",
            "idx_dyn"
        );
        assertThat(cause.getMessage(), containsString("field [features.topic_id] was resolved as type [keyword]"));
        assertThat(cause.getMessage(), containsString("mapped as incompatible type [integer] in index [idx_dyn]"));
    }

    /**
     * Creates idx_mapped with {@link #FIELD} mapped as {@code mappedType} (value 1), and idx_dyn where a dynamic
     * template maps the field as {@code dynamicType} once a doc containing it arrives.
     */
    private void setupIndices(String mappedType, String dynamicType) {
        assertAcked(
            prepareCreate("idx_mapped").setSettings(indexSettings(1, 0)).setMapping(FIELD, "type=" + mappedType, "other", "type=keyword")
        );
        createDynamicIndex("idx_dyn", dynamicType, null);
        indexInitialDocs(true);
    }

    private void createDynamicIndex(String index, String dynamicType, @Nullable String onNode) {
        var settings = indexSettings(1, 0);
        if (onNode != null) {
            settings.put("index.routing.allocation.require._name", onNode);
        }
        assertAcked(prepareCreate(index).setSettings(settings).setMapping(Strings.format("""
            {
              "dynamic_templates": [
                {
                  "topic_ids": {
                    "match": "topic_id",
                    "mapping": { "type": "%s" }
                  }
                }
              ],
              "properties": {
                "other": { "type": "keyword" }
              }
            }
            """, dynamicType)));
    }

    private void createPlainIndex(String index, @Nullable String onNode) {
        var settings = indexSettings(1, 0);
        if (onNode != null) {
            settings.put("index.routing.allocation.require._name", onNode);
        }
        assertAcked(prepareCreate(index).setSettings(settings).setMapping("other", "type=keyword"));
    }

    private void indexInitialDocs(boolean mappedIndex) {
        if (mappedIndex) {
            prepareIndex("idx_mapped").setSource(FIELD, 1, "other", "a").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        } else {
            prepareIndex("idx_plain").setSource("other", "a").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
        prepareIndex("idx_dyn").setSource("other", "b").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureGreen(mappedIndex ? "idx_mapped" : "idx_plain", "idx_dyn");
    }

    /**
     * Runs {@code query}, indexing a doc with {@code features.topic_id: 42} into {@code racingIndex} when the first
     * data-node request arrives — i.e. after field-caps resolution and coordinator planning.
     */
    private EsqlQueryResponse runWithMappingRace(String query, String racingIndex) {
        AtomicBoolean first = new AtomicBoolean();
        CountDownLatch mutationDone = new CountDownLatch(1);
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node)
                .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    if (first.compareAndSet(false, true)) {
                        prepareIndex(racingIndex).setSource("{ \"features\": { \"topic_id\": 42 }, \"other\": \"c\" }", XContentType.JSON)
                            .get();
                        indicesAdmin().prepareRefresh(racingIndex).get();
                        mutationDone.countDown();
                    } else {
                        assertTrue(mutationDone.await(30, TimeUnit.SECONDS));
                    }
                    handler.messageReceived(request, channel, task);
                });
        }
        try {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query(query);
            request.allowPartialResults(false);
            return run(request);
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
    }

    private ElasticsearchStatusException expectMappingRaceFailure(String query, String racingIndex) {
        Exception failure = expectThrows(Exception.class, () -> runWithMappingRace(query, racingIndex).close());
        var cause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(failure, ElasticsearchStatusException.class);
        assertNotNull("expected the shard to report the mapping mismatch, got: " + failure, cause);
        assertThat(cause.status(), equalTo(RestStatus.CONFLICT));
        return cause;
    }
}
