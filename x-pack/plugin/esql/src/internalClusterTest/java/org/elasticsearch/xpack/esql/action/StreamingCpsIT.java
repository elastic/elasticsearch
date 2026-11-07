/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StreamingCpsIT extends AbstractEsqlIntegTestCase {

    private static final int DOCS_IN_A = 3;
    private static final int DOCS_IN_B = 4;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ViewOverDatasetCpsIT.CpsSettingPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("serverless.cross_project.enabled", true).build();
    }

    @Before
    public void setupIndices() {
        assumeTrue("requires streaming", EsqlCapabilities.Cap.STREAMING.isEnabled());
        assertAcked(prepareCreate("cps-a").setMapping("populated", "type=keyword", "unpopulated", "type=keyword"));
        assertAcked(prepareCreate("cps-b").setMapping("populated", "type=keyword", "unpopulated", "type=keyword"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS_IN_A; i++) {
            bulk.add(new IndexRequest("cps-a").source("populated", "a" + i));
        }
        for (int i = 0; i < DOCS_IN_B; i++) {
            bulk.add(new IndexRequest("cps-b").source("populated", "b" + i));
        }
        assertNoFailures(bulk.get());
    }

    public void testStreamWildcardPattern() throws Exception {
        boolean dropNullColumns = randomBoolean();
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        EsqlStreamQueryAction.ResultStream resultStream = StreamQueryTestUtils.executeStreamRequest(
            client(),
            EsqlQueryRequest.syncEsqlQueryRequest("FROM cps-* | KEEP populated, unpopulated"),
            subscriber,
            dropNullColumns
        );

        assertThat(subscriber.rowCount.get(), equalTo(DOCS_IN_A + DOCS_IN_B));
        int populated = indexOfColumn(resultStream, "populated");
        int unpopulated = indexOfColumn(resultStream, "unpopulated");
        assertThat(populated, equalTo(0));
        assertThat(unpopulated, equalTo(1));
        if (dropNullColumns) {
            assertThat("nullColumns must be set when dropNullColumns=true", resultStream.nullColumns(), notNullValue());
            assertFalse(resultStream.nullColumns()[populated]);
            assertTrue(resultStream.nullColumns()[unpopulated]);
        }
        assertSuccessfulFooter(resultStream);
    }

    public void testStreamStats() throws Exception {
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        EsqlStreamQueryAction.ResultStream resultStream = StreamQueryTestUtils.executeStreamRequest(
            client(),
            EsqlQueryRequest.syncEsqlQueryRequest("FROM cps-* | STATS c = COUNT(*)"),
            subscriber
        );

        assertThat(subscriber.rowCount.get(), equalTo(1));
        assertThat(resultStream.columns().size(), equalTo(1));
        assertThat(resultStream.columns().get(0).name(), equalTo("c"));
        assertSuccessfulFooter(resultStream);
    }

    @SuppressWarnings("unchecked")
    public void testExecutionMetadataReportsOrigin() throws Exception {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest("FROM cps-* | STATS c = COUNT(*)");
        request.includeExecutionMetadata(true);
        EsqlStreamQueryAction.ResultStream resultStream = StreamQueryTestUtils.executeStreamRequest(
            client(),
            request,
            new StreamQueryTestUtils.CountingStreamSubscriber()
        );

        assertSuccessfulFooter(resultStream);
        PageStreamPublisher.StreamFooter footer = resultStream.publisher().footer();
        assertThat(footer.clusters(), notNullValue());
        Map<String, Object> clusters;
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(footer.clusters()))) {
            clusters = parser.map();
        }
        assertThat(clusters.get("total"), equalTo(1));
        assertThat(clusters.get("successful"), equalTo(1));
        assertThat(clusters.get("running"), equalTo(0));
        assertThat(clusters.get("skipped"), equalTo(0));
        assertThat(clusters.get("partial"), equalTo(0));
        assertThat(clusters.get("failed"), equalTo(0));
        Map<String, Object> details = (Map<String, Object>) clusters.get("details");
        assertThat(details.keySet(), equalTo(Set.of(EsqlExecutionInfo.ORIGIN_CLUSTER_NAME_REPRESENTATION)));
        Map<String, Object> origin = (Map<String, Object>) details.get(EsqlExecutionInfo.ORIGIN_CLUSTER_NAME_REPRESENTATION);
        assertThat(origin.get("status"), equalTo("successful"));
        assertThat(((Map<String, Object>) origin.get("_shards")).get("failed"), equalTo(0));
    }

    /**
     * A query that only touches the origin project is not a cross-cluster search, so {@code include_ccs_metadata} adds nothing.
     */
    public void testCcsMetadataOmittedForOriginOnlyQuery() throws Exception {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest("FROM cps-* | STATS c = COUNT(*)");
        request.includeCCSMetadata(randomBoolean());
        EsqlStreamQueryAction.ResultStream resultStream = StreamQueryTestUtils.executeStreamRequest(
            client(),
            request,
            new StreamQueryTestUtils.CountingStreamSubscriber()
        );

        assertSuccessfulFooter(resultStream);
        assertThat(resultStream.publisher().footer().clusters(), nullValue());
    }

    private static void assertSuccessfulFooter(EsqlStreamQueryAction.ResultStream resultStream) {
        PageStreamPublisher.StreamFooter footer = resultStream.publisher().footer();
        assertNotNull("footer must be present after the stream completes", footer);
        assertThat(footer.status(), equalTo(200));
        assertFalse(footer.isPartial());
        assertNull(footer.error());
    }

    private static int indexOfColumn(EsqlStreamQueryAction.ResultStream resultStream, String name) {
        for (int i = 0; i < resultStream.columns().size(); i++) {
            if (resultStream.columns().get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
