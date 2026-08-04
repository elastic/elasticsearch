/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Build;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end integration tests for {@link FallbackPostMapper} covering indexing and synthetic-source reconstruction.
 */
public class FallbackPostMapperIntegrationTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(ShardBatchIndexer.BATCH_INDEXING.getKey(), true).build();
    }

    /**
     * Bug: {@code GeoPointFieldMapper.multiFields().parse()} discards the
     * {@link FieldMapper.ParseResult.MultiValueViolation} from sub-fields, so values that should be
     * routed to {@code ._on_failure} are silently lost and the MVV is never recorded in {@code _ignored}.
     * This test FAILS currently.
     */
    public void testGeoPointMultiFieldMvvViolationRecorded() throws Exception {
        assumeTrue("doc_values on_failure feature flag must be enabled", FieldMapper.DOC_VALUES_ON_FAILURE_FEATURE_FLAG.isEnabled());

        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_point")
            .startObject("fields")
            .startObject("kw")
            .field("type", "keyword")
            .startObject("doc_values")
            .field("multi_value", false)
            .field("on_failure", "ignore")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        createIndex(
            "test-geopoint-mvv",
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
            mapping
        );

        client().index(
            new IndexRequest("test-geopoint-mvv").id("doc-1")
                .source(
                    jsonBuilder().startObject()
                        .startArray("location")
                        .startObject()
                        .field("lat", 1.0)
                        .field("lon", 2.0)
                        .endObject()
                        .startObject()
                        .field("lat", 3.0)
                        .field("lon", 4.0)
                        .endObject()
                        .endArray()
                        .endObject()
                )
        ).actionGet();

        client().execute(RefreshAction.INSTANCE, new RefreshRequest("test-geopoint-mvv")).actionGet();

        var docRequest = new SearchRequest("test-geopoint-mvv");
        docRequest.source().query(new IdsQueryBuilder().addIds("doc-1"));
        SearchResponse docResponse = client().search(docRequest).actionGet();
        try {
            assertThat(
                "document with multi-value geo_point sub-field must be indexed",
                docResponse.getHits().getTotalHits().value(),
                equalTo(1L)
            );
        } finally {
            docResponse.decRef();
        }

        var ignoredRequest = new SearchRequest("test-geopoint-mvv");
        ignoredRequest.source().query(new TermQueryBuilder("_ignored", "location.kw"));
        SearchResponse ignoredResponse = client().search(ignoredRequest).actionGet();
        try {
            assertThat("MVV violation must be recorded in _ignored", ignoredResponse.getHits().getTotalHits().value(), equalTo(1L));
        } finally {
            ignoredResponse.decRef();
        }
    }

    /**
     * A keyword with {@code doc_values:false, store:false} has {@link FieldMapper.SyntheticSourceMode#FALLBACK}:
     * its value reaches synthetic source only via {@code _ignored_source} through the pre-capture mechanism.
     * Verifies that the pre-capture is committed on {@link FieldMapper.ParseResult.Indexed}.
     */
    public void testSyntheticFallbackIndexedPreCaptureCommittedValueInSource() throws Exception {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .field("doc_values", false)
            .field("store", false)
            .endObject()
            .endObject()
            .endObject();

        createIndex("test-fallback-indexed", Settings.builder().put("index.mapping.source.mode", "synthetic").build(), mapping);

        client().index(
            new IndexRequest("test-fallback-indexed").id("doc-1").source(jsonBuilder().startObject().field("field", "hello").endObject())
        ).actionGet();

        client().execute(RefreshAction.INSTANCE, new RefreshRequest("test-fallback-indexed")).actionGet();

        var searchRequest = new SearchRequest("test-fallback-indexed");
        searchRequest.source().query(new IdsQueryBuilder().addIds("doc-1"));
        SearchResponse response = client().search(searchRequest).actionGet();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(response.getHits().getHits()[0].getSourceAsString(), equalTo("{\"field\":\"hello\"}"));
        } finally {
            response.decRef();
        }
    }

    /**
     * Same mapping as {@link #testSyntheticFallbackIndexedPreCaptureCommittedValueInSource} but with
     * {@code ignore_above} set so the value returns {@link FieldMapper.ParseResult.Ignored}.
     * Verifies that the pre-capture is still committed for FALLBACK fields on {@code Ignored}, not just {@code Indexed}.
     */
    public void testSyntheticFallbackIgnoredPreCaptureCommittedValueInSource() throws Exception {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .field("doc_values", false)
            .field("store", false)
            .field("ignore_above", 5)
            .endObject()
            .endObject()
            .endObject();

        createIndex("test-fallback-ignored", Settings.builder().put("index.mapping.source.mode", "synthetic").build(), mapping);

        client().index(
            new IndexRequest("test-fallback-ignored").id("doc-1")
                .source(jsonBuilder().startObject().field("field", "hello world").endObject())
        ).actionGet();

        client().execute(RefreshAction.INSTANCE, new RefreshRequest("test-fallback-ignored")).actionGet();

        var searchRequest = new SearchRequest("test-fallback-ignored");
        searchRequest.source().query(new IdsQueryBuilder().addIds("doc-1"));
        SearchResponse response = client().search(searchRequest).actionGet();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(
                "value exceeding ignore_above must be reconstructed from _ignored_source for FALLBACK fields",
                response.getHits().getHits()[0].getSourceAsString(),
                equalTo("{\"field\":\"hello world\"}")
            );
        } finally {
            response.decRef();
        }
    }

    /**
     * An integer field with {@code synthetic_source_keep:all} uses {@link FallbackPostMapper.Reason#SOURCE_KEEP_ALL}
     * for pre-capture. Verifies that a malformed value ({@code ignore_malformed}) is committed to
     * {@code _ignored_source} on {@link FieldMapper.ParseResult.Ignored}.
     */
    public void testSourceKeepAllIgnoredPreCaptureCommittedValueInSource() throws Exception {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "integer")
            .field("ignore_malformed", true)
            .field("synthetic_source_keep", "all")
            .endObject()
            .endObject()
            .endObject();

        createIndex("test-source-keep-all", Settings.builder().put("index.mapping.source.mode", "synthetic").build(), mapping);

        client().index(
            new IndexRequest("test-source-keep-all").id("doc-1")
                .source(jsonBuilder().startObject().field("field", "not-a-number").endObject())
        ).actionGet();

        client().execute(RefreshAction.INSTANCE, new RefreshRequest("test-source-keep-all")).actionGet();

        var searchRequest = new SearchRequest("test-source-keep-all");
        searchRequest.source().query(new IdsQueryBuilder().addIds("doc-1"));
        SearchResponse response = client().search(searchRequest).actionGet();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(
                "malformed value must appear in synthetic source via _ignored_source when pre-capture is committed",
                response.getHits().getHits()[0].getSourceAsString(),
                equalTo("{\"field\":\"not-a-number\"}")
            );
        } finally {
            response.decRef();
        }
    }

    /**
     * A {@code copy_to} destination field uses {@link FallbackPostMapper.Reason#COPY_TO_DESTINATION} for pre-capture.
     * Verifies that a direct malformed value (no copy-from source in the document) is committed to
     * {@code _ignored_source} on {@link FieldMapper.ParseResult.Ignored}.
     */
    public void testCopyToDestinationIgnoredPreCaptureCommittedValueInSource() throws Exception {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("src")
            .field("type", "keyword")
            .array("copy_to", "dest")
            .endObject()
            .startObject("dest")
            .field("type", "integer")
            .field("ignore_malformed", true)
            .endObject()
            .endObject()
            .endObject();

        createIndex("test-copy-to-dest", Settings.builder().put("index.mapping.source.mode", "synthetic").build(), mapping);

        client().index(
            new IndexRequest("test-copy-to-dest").id("doc-1").source(jsonBuilder().startObject().field("dest", "not-a-number").endObject())
        ).actionGet();

        client().execute(RefreshAction.INSTANCE, new RefreshRequest("test-copy-to-dest")).actionGet();

        var searchRequest = new SearchRequest("test-copy-to-dest");
        searchRequest.source().query(new IdsQueryBuilder().addIds("doc-1"));
        SearchResponse response = client().search(searchRequest).actionGet();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(
                "malformed value at a copy_to destination must appear in synthetic source via _ignored_source",
                response.getHits().getHits()[0].getSourceAsString(),
                equalTo("{\"dest\":\"not-a-number\"}")
            );
        } finally {
            response.decRef();
        }
    }

    /**
     * Bug: {@link org.elasticsearch.index.mapper.ShardBatchMapper#parseMappings} calls
     * {@code mapper.parse(ctx)} directly, bypassing {@link FallbackPostMapper#parseField}, so no pre-capture
     * is set up for FALLBACK fields and their values are silently absent from synthetic source.
     * This test FAILS currently.
     */
    public void testFallbackFieldValueLostInBatchPath() throws Exception {
        assumeTrue("batch indexing requires snapshot builds", Build.current().isSnapshot());
        assumeTrue("batch indexing feature flag must be enabled", ShardBatchIndexer.BATCH_INDEXING_FEATURE_FLAG.isEnabled());

        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .field("doc_values", false)
            .field("store", false)
            .endObject()
            .endObject()
            .endObject();

        createIndex("test-batch-fallback", Settings.builder().put("index.mapping.source.mode", "synthetic").build(), mapping);

        // BulkRequest triggers the EIRF batch path (ShardBatchMapper) when BATCH_INDEXING is enabled.
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
            new IndexRequest("test-batch-fallback").id("doc-1").source(jsonBuilder().startObject().field("field", "hello").endObject())
        );
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse("bulk indexing must not have failures", bulkResponse.hasFailures());

        client().execute(RefreshAction.INSTANCE, new RefreshRequest("test-batch-fallback")).actionGet();

        var searchRequest = new SearchRequest("test-batch-fallback");
        searchRequest.source().query(new IdsQueryBuilder().addIds("doc-1"));
        SearchResponse response = client().search(searchRequest).actionGet();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(
                "FALLBACK field value must appear in synthetic source via _ignored_source; "
                    + "lost because batch path bypasses FallbackPostMapper.parseField (ShardBatchMapper:259)",
                response.getHits().getHits()[0].getSourceAsString(),
                equalTo("{\"field\":\"hello\"}")
            );
        } finally {
            response.decRef();
        }
    }

    /**
     * Bug: when {@code postParse} sees {@link FieldMapper.ParseResult.Ignored} for a NATIVE-mode
     * {@code copy_to} destination, it discards the pre-capture (only FALLBACK mode commits on {@code Ignored}).
     * When a copy-from source also indexes into the destination, the malformed direct value is silently dropped
     * from synthetic source. This test FAILS currently.
     */
    public void testCopyToDestinationMalformedValueNotDroppedWhenCopyToSourcePresent() throws Exception {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("src")
            .field("type", "keyword")
            .array("copy_to", "dest")
            .endObject()
            .startObject("dest")
            .field("type", "integer")
            .field("ignore_malformed", true)
            .endObject()
            .endObject()
            .endObject();

        createIndex("test-copy-to-dest-with-src", Settings.builder().put("index.mapping.source.mode", "synthetic").build(), mapping);

        client().index(
            new IndexRequest("test-copy-to-dest-with-src").id("doc-1")
                .source(jsonBuilder().startObject().field("src", "123").field("dest", "not-a-number").endObject())
        ).actionGet();

        client().execute(RefreshAction.INSTANCE, new RefreshRequest("test-copy-to-dest-with-src")).actionGet();

        var searchRequest = new SearchRequest("test-copy-to-dest-with-src");
        searchRequest.source().query(new IdsQueryBuilder().addIds("doc-1"));
        SearchResponse response = client().search(searchRequest).actionGet();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(
                "malformed dest value must not be dropped from synthetic source when src also has a copy_to value",
                response.getHits().getHits()[0].getSourceAsString(),
                equalTo("{\"dest\":\"not-a-number\",\"src\":\"123\"}")
            );
        } finally {
            response.decRef();
        }
    }
}
