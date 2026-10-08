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
import org.elasticsearch.plugins.Plugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/** {@code INTO}: a query whose rows are written to a data stream instead of returned. */
public class IntoIT extends AbstractEsqlIntegTestCase {

    private static final String METRICS = "metrics-into";
    private static final String DESTINATION = "rollup-into";
    private static final long BASE_TIMESTAMP = 1_750_000_000_000L;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DataStreamsPlugin.class);
    }

    @Before
    public void createStreams() throws IOException {
        createStream(METRICS, """
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
                "max_cpu":    { "type": "double" },
                "samples":    { "type": "long" }
              }
            }
            """);
    }

    /** The rows go to the destination and the reply says how many. */
    public void testIntoWritesRowsAndReportsTheCount() {
        indexHosts(3, 100);

        final long written = number(
            "FROM "
                + METRICS
                + " | STATS max_cpu = MAX(cpu), samples = COUNT(*) BY host, `@timestamp` = DATE_TRUNC(1 hour, `@timestamp`) | INTO "
                + DESTINATION
        );
        assertThat(written, equalTo(3L));

        indicesAdmin().prepareRefresh(DESTINATION).get();
        assertThat(number("FROM " + DESTINATION + " | STATS c = COUNT(*)"), equalTo(3L));
        assertThat(
            "every source document must be accounted for",
            number("FROM " + DESTINATION + " | STATS s = SUM(samples)"),
            equalTo(100L)
        );
    }

    /**
     * No row cap applies: a cap would make the write silently incomplete rather than truncate a display. The group
     * count sits above the default cap so that reinstating one fails this test.
     */
    public void testEveryGroupIsWrittenHoweverManyThereAre() {
        final int hosts = 2500;
        indexHosts(hosts, hosts);

        assertThat(
            "every group must be written",
            number(
                "FROM "
                    + METRICS
                    + " | STATS max_cpu = MAX(cpu) BY host, `@timestamp` = DATE_TRUNC(1 hour, `@timestamp`) | INTO "
                    + DESTINATION
            ),
            equalTo((long) hosts)
        );
        indicesAdmin().prepareRefresh(DESTINATION).get();
        assertThat(number("FROM " + DESTINATION + " | STATS c = COUNT(*)"), equalTo((long) hosts));
    }

    /** Rows are appended, never updated, so running the same query twice leaves both sets of rows. */
    public void testRunningItAgainAppends() {
        indexHosts(3, 60);

        final long first = number(
            "FROM " + METRICS + " | STATS max_cpu = MAX(cpu) BY host, `@timestamp` = DATE_TRUNC(1 hour, `@timestamp`) | INTO " + DESTINATION
        );
        assertThat(first, greaterThan(0L));
        final long second = number(
            "FROM " + METRICS + " | STATS max_cpu = MAX(cpu) BY host, `@timestamp` = DATE_TRUNC(1 hour, `@timestamp`) | INTO " + DESTINATION
        );
        assertThat(second, equalTo(first));

        indicesAdmin().prepareRefresh(DESTINATION).get();
        assertThat(number("FROM " + DESTINATION + " | STATS c = COUNT(*)"), equalTo(first * 2));
    }

    /** A query that produced nothing writes nothing, and says so rather than failing. */
    public void testAnEmptyResultWritesNothing() {
        indexHosts(3, 30);

        assertThat(
            number(
                "FROM "
                    + METRICS
                    + " | WHERE host == \"absent\" | STATS max_cpu = MAX(cpu) BY host, `@timestamp` = DATE_TRUNC(1 hour, `@timestamp`) | INTO "
                    + DESTINATION
            ),
            equalTo(0L)
        );
    }

    /**
     * A destination has to exist or be described by a template. A name matching neither would otherwise be created by
     * the write, dynamically mapped from whatever the first rows held, so a mistyped name would succeed.
     */
    public void testWritingToAMissingDestinationFails() {
        indexHosts(3, 10);

        final var failure = expectThrows(
            Exception.class,
            () -> number(
                "FROM "
                    + METRICS
                    + " | STATS max_cpu = MAX(cpu) BY host, `@timestamp` = DATE_TRUNC(1 hour, `@timestamp`)"
                    + " | INTO no-such-destination"
            )
        );
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        assertThat(cause.getMessage(), containsString("no index template matches it"));
    }

    private void indexHosts(int hosts, int documents) {
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < documents; i++) {
            bulk.add(
                client().prepareIndex(METRICS)
                    .setOpType(DocWriteRequest.OpType.CREATE)
                    .setSource("@timestamp", BASE_TIMESTAMP, "host", "host-" + (i % hosts), "cpu", (double) randomIntBetween(1, 100))
            );
        }
        final BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        indicesAdmin().prepareRefresh(METRICS).get();
    }

    /** Runs a query returning a single number. */
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
        // A data stream only exists once something has been written to it, and INTO writes to it rather than
        // creating it, so it is rolled over into existence here.
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name)
            )
        );
    }
}
