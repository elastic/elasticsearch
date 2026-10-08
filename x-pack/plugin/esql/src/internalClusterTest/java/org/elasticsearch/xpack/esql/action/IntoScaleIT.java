/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * How {@code INTO} behaves as what it is asked to write grows. Two regimes, with different limits: without a reduce
 * rows stream through and nothing is held between them, while with one the aggregation holds every group until the
 * last row has been read, so the cost follows the number of groups.
 * <p>
 * Skipped unless asked for, because the sizes that say anything take longer than a test should. Run one with
 * {@code -Dtests.into.scale=true -Dtests.into.size=1000000 -Dtests.heap.size=4g}; the heap is shared by every node of
 * the test cluster, so writing the source competes with the query for it. {@code tests.into.rows} sets the document
 * count and {@code tests.into.size} the group count, so they can be raised one at a time.
 */
public class IntoScaleIT extends AbstractEsqlIntegTestCase {

    private static final Logger logger = LogManager.getLogger(IntoScaleIT.class);

    private static final String SOURCE = "scale-source";
    private static final String DESTINATION = "rollup-scale";
    private static final long BASE_TIMESTAMP = 1_750_000_000_000L;
    private static final int BULK_SIZE = 10_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DataStreamsPlugin.class);
    }

    @Before
    public void onlyWhenAsked() throws IOException {
        assumeTrue("scale probes are opt-in: -Dtests.into.scale=true", Boolean.getBoolean("tests.into.scale"));
        createStream(SOURCE, """
            {
              "properties": {
                "@timestamp": { "type": "date" },
                "host":       { "type": "keyword" },
                "cpu":        { "type": "double" }
              }
            }
            """);
        createStream(DESTINATION, """
            {
              "properties": {
                "@timestamp": { "type": "date" },
                "host":       { "type": "keyword" },
                "cpu":        { "type": "double" },
                "max_cpu":    { "type": "double" }
              }
            }
            """);
    }

    /**
     * A reindex: every row is written, none is grouped with any other. Nothing needs to be held, so this is expected
     * to hold at any size - which is what makes it the case to reach for when a rollup will not fit.
     */
    public void testReindexingManyRows() {
        final int rows = rows();
        index(groups(), rows);

        final long written = number("FROM " + SOURCE + " | KEEP `@timestamp`, host, cpu | INTO " + DESTINATION);
        logger.info("INTO SCALE reindex rows={} written={}", rows, written);
        assertThat(written, equalTo((long) rows));
    }

    /** A rollup whose every row is its own group, so the aggregation holds the whole result before anything is written. */
    public void testRollingUpManyGroups() {
        final int groups = groups();
        index(groups, rows());

        final long written = number(
            "FROM " + SOURCE + " | STATS max_cpu = MAX(cpu) BY host, `@timestamp` = DATE_TRUNC(1 hour, `@timestamp`) | INTO " + DESTINATION
        );
        logger.info("INTO SCALE rollup groups={} written={}", groups, written);
        assertThat(written, equalTo((long) groups));
    }

    /**
     * The same rollup as several passes over disjoint slices of the groups. Each pass holds only its slice, so the
     * memory needed is chosen rather than given, at the cost of reading the source once per pass.
     */
    public void testRollingUpManyGroupsInSlices() {
        final int groups = groups();
        final int slices = Integer.parseInt(System.getProperty("tests.into.slices", "8"));
        index(groups, rows());

        long written = 0;
        for (int slice = 0; slice < slices; slice++) {
            written += number(
                "FROM "
                    + SOURCE
                    + " | WHERE ABS(HASH(\"md5\", host)) % "
                    + slices
                    + " == "
                    + slice
                    + " | STATS max_cpu = MAX(cpu) BY host, `@timestamp` = DATE_TRUNC(1 hour, `@timestamp`)"
                    + " | INTO "
                    + DESTINATION
            );
        }
        logger.info("INTO SCALE sliced rollup groups={} slices={} written={}", groups, slices, written);
        assertThat(written, equalTo((long) groups));
    }

    /** How many groups the reduce has to hold. */
    private static int groups() {
        return Integer.parseInt(System.getProperty("tests.into.size", "100000"));
    }

    /** How many documents to write. Separate from the group count so the two can be varied one at a time. */
    private static int rows() {
        return Integer.parseInt(System.getProperty("tests.into.rows", String.valueOf(groups())));
    }

    private void index(int hosts, int documents) {
        for (int written = 0; written < documents; written += BULK_SIZE) {
            final BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = written; i < Math.min(written + BULK_SIZE, documents); i++) {
                bulk.add(
                    client().prepareIndex(SOURCE)
                        .setOpType(DocWriteRequest.OpType.CREATE)
                        .setSource("@timestamp", BASE_TIMESTAMP, "host", "host-" + (i % hosts), "cpu", (double) (i % 100))
                );
            }
            final BulkResponse response = bulk.get();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
        indicesAdmin().prepareRefresh(SOURCE).get();
    }

    private long number(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query))) {
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }

    private void createStream(String name, String mappings) throws IOException {
        final ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(name + "*"))
            .template(
                Template.builder()
                    .settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                    .mappings(CompressedXContent.fromJSON(mappings))
            )
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        final var request = new TransportPutComposableIndexTemplateAction.Request(name + "-template");
        request.indexTemplate(template);
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name)
            )
        );
    }
}
