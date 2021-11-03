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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.TimeSeriesIdGenerator;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesIdFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return TimeSeriesIdFieldMapper.NAME;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // There aren't any parameters
    }

    public void testEnabledInTimeSeriesMode() throws Exception {
        DocumentMapper docMapper = createMapperService(
            getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "kw_field")
                .build(),
            mapping(b -> {
                b.startObject("kw_field");
                b.field("type", "keyword");
                b.field("time_series_dimension", true);
                b.endObject();
                b.startObject("long_field");
                b.field("type", "long");
                b.field("time_series_dimension", true);
                b.endObject();
            })
        ).documentMapper();

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("kw_field", "value")
                        .field("long_field", 100)
                        .field("label_field", 500)
                        .field("@timestamp", "2021-10-01")
                        .endObject()
                ),
                XContentType.JSON,
                null,
                Map.of()
            )
        );

        assertThat(
            doc.rootDoc().getBinaryValue("_tsid"),
            equalTo(new BytesRef("\u0002\bkw_fields\u0005value\n" + "long_fieldl\u0000\u0000\u0000\u0000\u0000\u0000\u0000d"))
        );
        assertThat(doc.rootDoc().getField("kw_field").binaryValue(), equalTo(new BytesRef("value")));
        assertThat(doc.rootDoc().getField("long_field").numericValue(), equalTo(100L));

        // TODo: Remove
        TimeSeriesIdGenerator.parse(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes));
    }

    public void testDisabledInStandardMode() throws Exception {
        DocumentMapper docMapper = createMapperService(
            getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), "standard").build(),
            mapping(b -> {})
        ).documentMapper();

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject()),
                XContentType.JSON,
                null,
                Map.of()
            )
        );

        assertThat(doc.rootDoc().getBinaryValue("_tsid"), is(nullValue()));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInDocumentNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source(b -> b.field("_tsid", "foo"))));

        assertThat(e.getCause().getMessage(), containsString("Field [_tsid] is a metadata field and cannot be added inside a document"));
    }
}
