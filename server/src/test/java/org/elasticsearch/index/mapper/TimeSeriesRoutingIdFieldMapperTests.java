/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesRoutingIdFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return TimeSeriesRoutingIdFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // There aren't any parameters
    }

    private DocumentMapper createMapper(XContentBuilder mappings) throws IOException {
        return createMapperService(
            getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "routing path is required")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build(),
            mappings
        ).documentMapper();
    }

    private static ParsedDocument parseDocument(int hash, DocumentMapper docMapper, CheckedConsumer<XContentBuilder, IOException> f)
        throws IOException {
        // Add the @timestamp field required by DataStreamTimestampFieldMapper for all time series indices
        return docMapper.parse(source(TimeSeriesRoutingIdFieldMapper.encode(hash), b -> {
            f.accept(b);
            b.field("@timestamp", "2021-10-01");
        }, null));
    }

    private static int getRoutingId(ParsedDocument document) {
        BytesRef value = document.rootDoc().getBinaryValue(TimeSeriesRoutingIdFieldMapper.NAME);
        return TimeSeriesRoutingIdFieldMapper.decode(Uid.decodeId(value.bytes));
    }

    @SuppressWarnings("unchecked")
    public void testEnabledInTimeSeriesMode() throws Exception {
        DocumentMapper docMapper = createMapper(mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));

        int hash = randomInt();
        ParsedDocument doc = parseDocument(hash, docMapper, b -> b.field("a", "value"));
        assertThat(doc.rootDoc().getField("a").binaryValue(), equalTo(new BytesRef("value")));
        assertEquals(hash, getRoutingId(doc));
    }

    public void testDisabledInStandardMode() throws Exception {
        DocumentMapper docMapper = createMapperService(
            getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.name()).build(),
            mapping(b -> {})
        ).documentMapper();
        assertThat(docMapper.metadataMapper(TimeSeriesRoutingIdFieldMapper.class), is(nullValue()));

        ParsedDocument doc = docMapper.parse(source("id", b -> b.field("field", "value"), null));
        assertThat(doc.rootDoc().getBinaryValue("_routing_id"), is(nullValue()));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInDocumentNotAllowed() throws Exception {
        DocumentMapper docMapper = createMapper(mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> parseDocument(randomInt(), docMapper, b -> b.field("_routing_id", "foo"))
        );

        assertThat(
            e.getCause().getMessage(),
            containsString("Field [_routing_id] is a metadata field and cannot be added inside a document")
        );
    }
}
