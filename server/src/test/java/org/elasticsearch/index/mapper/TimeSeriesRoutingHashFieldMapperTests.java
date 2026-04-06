/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesRoutingHashFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return TimeSeriesRoutingHashFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // There aren't any parameters
    }

    @Override
    protected Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
    }

    private DocumentMapper createMapper(XContentBuilder mappings) throws IOException {
        return createMapper(mappings, null);
    }

    /**
     * Create a DocumentMapper for the test. Set syntheticId setting explicitly if true/false. Leave unset if null.
     */
    private DocumentMapper createMapper(XContentBuilder mappings, Boolean syntheticId) throws IOException {
        Settings.Builder settingsBuilder = getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "routing path is required")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-10-29T00:00:00Z");
        if (syntheticId != null) {
            settingsBuilder.put(IndexSettings.SYNTHETIC_ID.getKey(), syntheticId);
        }
        return createMapperService(settingsBuilder.build(), mappings).documentMapper();
    }

    private static ParsedDocument parseDocument(int hash, DocumentMapper docMapper, CheckedConsumer<XContentBuilder, IOException> f)
        throws IOException {
        // Add the @timestamp field required by DataStreamTimestampFieldMapper for all time series indices
        return docMapper.parse(source(null, b -> {
            f.accept(b);
            b.field("@timestamp", "2021-10-01");
        }, TimeSeriesRoutingHashFieldMapper.encode(hash)));
    }

    private static ParsedDocument parseDocument(String id, DocumentMapper docMapper, CheckedConsumer<XContentBuilder, IOException> f)
        throws IOException {
        // Add the @timestamp field required by DataStreamTimestampFieldMapper for all time series indices
        return docMapper.parse(source(id, b -> {
            f.accept(b);
            b.field("@timestamp", "2021-10-01");
        }, null));
    }

    private static int getRoutingHash(ParsedDocument document) {
        BytesRef value = document.rootDoc().getBinaryValue(TimeSeriesRoutingHashFieldMapper.NAME);
        return TimeSeriesRoutingHashFieldMapper.decode(Uid.decodeId(value.bytes));
    }

    @SuppressWarnings("unchecked")
    public void testEnabledInTimeSeriesMode() throws Exception {
        DocumentMapper docMapper = createMapper(mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));

        int hash = randomInt();
        ParsedDocument doc = parseDocument(hash, docMapper, b -> b.field("a", "value"));
        assertThat(doc.rootDoc().getField("a").binaryValue(), equalTo(new BytesRef("value")));
        assertEquals(hash, getRoutingHash(doc));
    }

    public void testRetrievedFromIdInTimeSeriesMode() throws Exception {
        boolean syntheticId = IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG && randomBoolean();
        DocumentMapper docMapper = createMapper(mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
        }), syntheticId);

        String id;
        int routingHash = randomInt();
        if (syntheticId) {
            // This synthetic id is not correct for the document, but we only care about the routing hash in this test
            id = TsidExtractingIdFieldMapper.createSyntheticId(new BytesRef("some tsid"), Instant.now().toEpochMilli(), routingHash);
        } else {
            id = TimeSeriesRoutingHashFieldMapper.encode(routingHash);
        }
        ParsedDocument doc = parseDocument(id, docMapper, b -> b.field("a", "value"));
        assertThat(doc.rootDoc().getField("a").binaryValue(), equalTo(new BytesRef("value")));
        assertEquals(routingHash, getRoutingHash(doc));
    }

    public void testDisabledInStandardMode() throws Exception {
        Settings.Builder builder = getIndexSettingsBuilder();
        builder.put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.name());
        builder.remove(IndexMetadata.INDEX_ROUTING_PATH.getKey());
        DocumentMapper docMapper = createMapperService(builder.build(), mapping(b -> {})).documentMapper();
        assertThat(docMapper.metadataMapper(TimeSeriesRoutingHashFieldMapper.class), is(nullValue()));

        ParsedDocument doc = docMapper.parse(source("id", b -> b.field("field", "value"), null));
        assertThat(doc.rootDoc().getBinaryValue("_ts_routing_hash"), is(nullValue()));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInDocumentNotAllowed() throws Exception {
        DocumentMapper docMapper = createMapper(mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> parseDocument(randomInt(), docMapper, b -> b.field("_ts_routing_hash", "foo"))
        );

        assertThat(
            e.getCause().getMessage(),
            containsString("Field [_ts_routing_hash] is a metadata field and cannot be added inside a document")
        );
    }
}
