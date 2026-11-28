/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.UpdateDataStreamSettingsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Slice-enabled indices used as the backing indices of a data stream. Data streams add three things a plain slice index
 * does not have: writes are rewritten to the current write index, searches resolve to every backing index, and the
 * write path rejects custom routing unless the template opts in, which a slice write is.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class DataStreamSliceIT extends ESIntegTestCase {

    private static final String TEMPLATE = "slice-template";

    @Before
    public void requireSliceFeatureFlag() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    /**
     * Creates a data stream template matching {@code pattern}.
     *
     * @param sliceEnabled       value of {@code index.slice.enabled} for the backing indices
     * @param allowCustomRouting whether the template sets {@code allow_custom_routing}
     */
    private static void putTemplate(String pattern, boolean sliceEnabled, boolean allowCustomRouting) throws IOException {
        putTemplate(pattern, sliceEnabled, allowCustomRouting, Settings.EMPTY);
    }

    private static void putTemplate(String pattern, boolean sliceEnabled, boolean allowCustomRouting, Settings extraSettings)
        throws IOException {
        putTemplate(TEMPLATE, pattern, 0L, sliceEnabled, allowCustomRouting, extraSettings);
    }

    private static void putTemplate(
        String name,
        String pattern,
        long priority,
        boolean sliceEnabled,
        boolean allowCustomRouting,
        Settings extraSettings
    ) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(name);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(pattern))
                .priority(priority)
                .template(
                    Template.builder()
                        .settings(
                            Settings.builder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                                .put(IndexSettings.SLICE_ENABLED.getKey(), sliceEnabled)
                                .put(extraSettings)
                        )
                        .mappings(CompressedXContent.fromJSON("""
                            { "properties": { "@timestamp": { "type": "date" }, "value": { "type": "keyword" } } }"""))
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, allowCustomRouting))
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet());
    }

    private static void putComponentTemplate(String name, boolean sliceEnabled) {
        PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request(name);
        request.componentTemplate(
            new ComponentTemplate(
                new Template(
                    Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), sliceEnabled).put("index.number_of_shards", 1).build(),
                    null,
                    null
                ),
                null,
                null
            )
        );
        assertAcked(client().execute(PutComponentTemplateAction.INSTANCE, request).actionGet());
    }

    private static void createDataStream(String name) {
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name)
            ).actionGet()
        );
    }

    private static IndexRequest createRequest(String dataStream, String slice, String id, String value) {
        IndexRequest request = new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
            .source("{\"@timestamp\": \"2020-12-12\", \"value\": \"" + value + "\"}", XContentType.JSON)
            .routing(slice)
            .setRoutingFromSlice(true);
        if (id != null) {
            request.id(id);
        }
        return request;
    }

    private SearchRequestBuilder searchSlice(String target, String slice) {
        SearchRequestBuilder search = prepareSearch(target);
        search.request().searchSlice(slice);
        return search;
    }

    private SearchRequestBuilder searchSliceOn(String slice, String... targets) {
        SearchRequestBuilder search = prepareSearch(targets);
        search.request().searchSlice(slice);
        return search;
    }

    private static Set<String> values(org.elasticsearch.action.search.SearchResponse response) {
        Set<String> values = new HashSet<>();
        for (SearchHit hit : response.getHits().getHits()) {
            values.add((String) hit.getSourceAsMap().get("value"));
        }
        return values;
    }

    private void rollover(String dataStream) {
        RolloverResponse response = indicesAdmin().rolloverIndex(new RolloverRequest(dataStream, null)).actionGet();
        assertTrue("rollover must succeed", response.isRolledOver());
    }

    /**
     * The settings come from the template: the backing index is slice-enabled, a slice write lands in it, and a search
     * through the data stream sees only the requested slice.
     */
    public void testSliceWriteAndSearch() throws Exception {
        putTemplate("slice-ds*", true, true);
        createDataStream("slice-ds");

        DocWriteResponse a = client().index(createRequest("slice-ds", "a", "1", "va")).actionGet();
        DocWriteResponse b = client().index(createRequest("slice-ds", "b", "1", "vb")).actionGet();
        assertThat(a.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(b.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat("responses report the plain id", a.getId(), equalTo("1"));
        assertThat(a.getIndex(), equalTo(DataStream.getDefaultBackingIndexName("slice-ds", 1)));
        refresh("slice-ds");

        assertResponse(searchSlice("slice-ds", "a"), response -> {
            assertHitCount(response, 1);
            assertThat(values(response), equalTo(Set.of("va")));
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        });
        assertResponse(searchSlice("slice-ds", "b"), response -> assertThat(values(response), equalTo(Set.of("vb"))));
        assertResponse(searchSlice("slice-ds", "a,b"), response -> assertThat(values(response), equalTo(Set.of("va", "vb"))));
        assertResponse(searchSlice("slice-ds", SliceIndexing.SLICE_ALL), response -> assertHitCount(response, 2));
        assertResponse(searchSlice("slice-ds", "unused"), response -> assertHitCount(response, 0));
    }

    /**
     * A slice write is routed with the slice, so a slice-enabled data stream must allow custom routing. A template without
     * {@code allow_custom_routing} is rejected when it is put, because it makes the backing indices' {@code _routing} not
     * required, which a slice-enabled index forbids.
     */
    public void testSliceEnabledTemplateRequiresAllowCustomRouting() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> putTemplate("noroute-ds*", true, false));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("must not configure [_routing] settings when [index.slice.enabled]"));
    }

    /**
     * A slice-enabled backing index requires every write to name its slice, also when it arrives through the data stream.
     */
    public void testWriteWithoutSliceIsRejected() throws Exception {
        putTemplate("slice-ds*", true, true);
        createDataStream("slice-ds");

        IndexRequest noSlice = new IndexRequest("slice-ds").opType(DocWriteRequest.OpType.CREATE)
            .source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().index(noSlice).actionGet());
        assertThat(e.getMessage(), containsString("[_slice] is required"));
    }

    /**
     * A bulk request mixing slices is written through the data stream; the same explicit id in two slices yields two
     * documents, and a repeated create within one slice conflicts.
     */
    public void testBulkAcrossSlices() throws Exception {
        putTemplate("slice-ds*", true, true);
        createDataStream("slice-ds");

        BulkRequest bulk = new BulkRequest();
        bulk.add(createRequest("slice-ds", "a", "1", "a1"));
        bulk.add(createRequest("slice-ds", "b", "1", "b1"));
        bulk.add(createRequest("slice-ds", "a", null, "a-auto"));
        bulk.add(createRequest("slice-ds", "a", "1", "a1-again"));
        BulkResponse response = client().bulk(bulk).actionGet();

        BulkItemResponse[] items = response.getItems();
        assertThat(items[0].isFailed(), equalTo(false));
        assertThat(items[1].isFailed(), equalTo(false));
        assertThat(items[2].isFailed(), equalTo(false));
        assertThat("create of an existing (slice, id) conflicts", items[3].isFailed(), equalTo(true));
        assertThat(items[3].getFailure(), notNullValue());
        assertThat(items[3].getFailureMessage(), containsString("version conflict"));
        refresh("slice-ds");

        assertResponse(searchSlice("slice-ds", "a"), r -> assertThat(values(r), equalTo(Set.of("a1", "a-auto"))));
        assertResponse(searchSlice("slice-ds", "b"), r -> assertThat(values(r), equalTo(Set.of("b1"))));
    }

    /**
     * After a rollover the new write index inherits the slice setting from the template, and a slice search spans both
     * generations.
     */
    public void testSliceSearchSpansRollover() throws Exception {
        putTemplate("slice-ds*", true, true);
        createDataStream("slice-ds");

        client().index(createRequest("slice-ds", "a", null, "gen1-a")).actionGet();
        client().index(createRequest("slice-ds", "b", null, "gen1-b")).actionGet();
        rollover("slice-ds");
        DocWriteResponse gen2 = client().index(createRequest("slice-ds", "a", null, "gen2-a")).actionGet();
        assertThat(gen2.getIndex(), equalTo(DataStream.getDefaultBackingIndexName("slice-ds", 2)));
        client().index(createRequest("slice-ds", "c", null, "gen2-c")).actionGet();
        refresh("slice-ds");

        assertResponse(searchSlice("slice-ds", "a"), r -> assertThat(values(r), equalTo(Set.of("gen1-a", "gen2-a"))));
        assertResponse(searchSlice("slice-ds", "b"), r -> assertThat(values(r), equalTo(Set.of("gen1-b"))));
        assertResponse(searchSlice("slice-ds", "c"), r -> assertThat(values(r), equalTo(Set.of("gen2-c"))));
        assertResponse(searchSlice("slice-ds", SliceIndexing.SLICE_ALL), r -> assertHitCount(r, 4));
    }

    /**
     * {@code index.slice.enabled} is final per index, so a template change would only reach later backing indices and leave the
     * stream mixing indices with and without slices. The change is rejected while a data stream uses the template.
     */
    public void testTemplateCannotChangeSliceForExistingDataStream() throws Exception {
        putTemplate("slice-ds*", true, true);
        createDataStream("slice-ds");

        Exception e = expectThrows(Exception.class, () -> putTemplate("slice-ds*", false, true));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("composable template [slice-template]"));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("would change [index.slice.enabled] to [false] for data streams"));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("slice-ds"));

        // an unrelated change to the same template is still accepted
        putTemplate("slice-ds*", true, true, Settings.builder().put("index.refresh_interval", "5s").build());
    }

    /**
     * A template update or deletion cannot hand an existing data stream to a fallback template with a different slice setting.
     */
    public void testWinningTemplateCannotChangeSliceForExistingDataStream() throws Exception {
        putTemplate("fallback", "slice-ds*", 100L, false, true, Settings.EMPTY);
        putTemplate("winner", "slice-ds*", 200L, true, true, Settings.EMPTY);
        createDataStream("slice-ds");

        Exception update = expectThrows(Exception.class, () -> putTemplate("winner", "other-ds*", 200L, true, true, Settings.EMPTY));
        assertThat(ExceptionsHelper.stackTrace(update), containsString("composable template [winner]"));
        assertThat(ExceptionsHelper.stackTrace(update), containsString("would change [index.slice.enabled] to [false]"));

        Exception delete = expectThrows(
            Exception.class,
            client().execute(
                TransportDeleteComposableIndexTemplateAction.TYPE,
                new TransportDeleteComposableIndexTemplateAction.Request("winner")
            )
        );
        assertThat(ExceptionsHelper.stackTrace(delete), containsString("removing composable templates [winner]"));
        assertThat(ExceptionsHelper.stackTrace(delete), containsString("would change [index.slice.enabled] to [false]"));

        // Deleting the winner is safe once the fallback has the same setting.
        putTemplate("fallback", "slice-ds*", 100L, true, true, Settings.EMPTY);
        assertAcked(
            client().execute(
                TransportDeleteComposableIndexTemplateAction.TYPE,
                new TransportDeleteComposableIndexTemplateAction.Request("winner")
            ).actionGet()
        );
    }

    /**
     * The same holds when the setting comes from a component template and not from the composable template itself.
     */
    public void testComponentTemplateCannotChangeSliceForExistingDataStream() throws Exception {
        putComponentTemplate("slice-component", true);
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(TEMPLATE);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("slice-ds*"))
                .componentTemplates(List.of("slice-component"))
                .template(Template.builder().settings(Settings.builder().put("index.number_of_replicas", 0)))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, true))
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet());
        createDataStream("slice-ds");

        Exception e = expectThrows(Exception.class, () -> putComponentTemplate("slice-component", false));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("component template [slice-component]"));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("would change [index.slice.enabled] to [false] for data streams"));
    }

    /**
     * Removing an optional component template cannot change the slice setting used by the next backing index.
     */
    public void testOptionalComponentTemplateCannotBeRemovedFromExistingDataStream() throws Exception {
        putComponentTemplate("slice-component", true);
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(TEMPLATE);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("slice-ds*"))
                .componentTemplates(List.of("slice-component"))
                .ignoreMissingComponentTemplates(List.of("slice-component"))
                .template(Template.builder().settings(Settings.builder().put("index.number_of_replicas", 0)))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, true))
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet());
        createDataStream("slice-ds");

        Exception e = expectThrows(
            Exception.class,
            client().execute(
                TransportDeleteComponentTemplateAction.TYPE,
                new TransportDeleteComponentTemplateAction.Request("slice-component")
            )
        );
        assertThat(ExceptionsHelper.stackTrace(e), containsString("removing component templates [slice-component]"));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("would change [index.slice.enabled] to [false]"));
    }

    /**
     * Changing the template is fine when no data stream uses it yet: a new data stream follows the new setting while the
     * previous one keeps its own.
     */
    public void testNewDataStreamFollowsChangedTemplateAfterOldOneIsDeleted() throws Exception {
        putTemplate("slice-ds*", true, true);
        createDataStream("slice-ds");
        assertAcked(
            client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, "slice-ds"))
                .actionGet()
        );

        putTemplate("slice-ds*", false, true);
        createDataStream("slice-ds-2");
        assertThat(
            indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, "slice-ds-2")
                .get()
                .getIndexToSettings()
                .values()
                .iterator()
                .next()
                .getAsBoolean("index.slice.enabled", false),
            equalTo(false)
        );
    }

    /**
     * The data stream settings API only accepts a fixed set of settings, which does not include {@code index.slice.enabled}.
     */
    public void testDataStreamSettingsApiRejectsSliceEnabled() throws Exception {
        putTemplate("slice-ds*", false, true);
        createDataStream("slice-ds");

        var response = client().execute(
            UpdateDataStreamSettingsAction.INSTANCE,
            new UpdateDataStreamSettingsAction.Request(
                Settings.builder().put("index.slice.enabled", true).build(),
                TimeValue.THIRTY_SECONDS,
                TimeValue.THIRTY_SECONDS
            ).indices("slice-ds")
        ).actionGet();
        var result = response.getDataStreamSettingsResponses().get(0);
        assertThat(result.dataStreamSucceeded(), equalTo(false));
        assertThat(
            result.dataStreamErrorMessage(),
            containsString("Cannot set the following settings on a data stream: [index.slice.enabled]")
        );
    }

    /**
     * A slice search over a slice data stream and a plain index skips the plain index, which holds no slice documents; an
     * {@code _all} search still sees both.
     */
    public void testSliceSearchSkipsPlainIndexAlongsideDataStream() throws Exception {
        putTemplate("slice-ds*", true, true);
        createDataStream("slice-ds");
        client().index(createRequest("slice-ds", "a", null, "ds-a")).actionGet();
        assertAcked(prepareCreate("plain-idx").setMapping("value", "type=keyword"));
        client().prepareIndex("plain-idx").setSource("value", "plain").get();
        refresh("slice-ds", "plain-idx");

        assertResponse(searchSliceOn("a", "slice-ds", "plain-idx"), r -> {
            assertNoFailures(r);
            assertThat(values(r), equalTo(Set.of("ds-a")));
        });
        assertResponse(searchSliceOn(SliceIndexing.SLICE_ALL, "slice-ds", "plain-idx"), r -> {
            assertNoFailures(r);
            assertThat(values(r), equalTo(Set.of("ds-a", "plain")));
        });
    }

    /**
     * A slice search can address one backing index directly; only that generation's documents match.
     */
    public void testSliceSearchOnBackingIndex() throws Exception {
        putTemplate("slice-ds*", true, true);
        createDataStream("slice-ds");
        client().index(createRequest("slice-ds", "a", null, "gen1-a")).actionGet();
        rollover("slice-ds");
        client().index(createRequest("slice-ds", "a", null, "gen2-a")).actionGet();
        refresh("slice-ds");

        assertResponse(
            searchSlice(DataStream.getDefaultBackingIndexName("slice-ds", 1), "a"),
            r -> assertThat(values(r), equalTo(Set.of("gen1-a")))
        );
        assertResponse(
            searchSlice(DataStream.getDefaultBackingIndexName("slice-ds", 2), "a"),
            r -> assertThat(values(r), equalTo(Set.of("gen2-a")))
        );
    }

    /**
     * Time series data streams route on their dimensions, which conflicts with slice routing, so the combination is
     * rejected when the template is put.
     */
    public void testSliceEnabledTimeSeriesTemplateIsRejected() {
        Exception e = expectThrows(
            InvalidIndexTemplateException.class,
            () -> putTemplate(
                "tsdb-ds*",
                true,
                true,
                Settings.builder().put("index.mode", "time_series").putList("index.routing_path", "value").build()
            )
        );
        assertThat(ExceptionsHelper.stackTrace(e), containsString("[index.slice.enabled] cannot be used with [index.mode=time_series]"));
    }
}
