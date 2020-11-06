/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Base class for testing cartesian field mappers */
public abstract class CartesianFieldMapperTests  extends MapperTestCase {

    static final String FIELD_NAME = "field";

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new LocalStateSpatialPlugin());
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", getFieldName());
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "POINT (14.0 15.0)";
    }

    protected abstract String getFieldName();


    public void testWKT() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "POINT (2000.1 305.6)")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }


    public void testInvalidPointValuesIgnored() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_malformed", true);
        }));

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "1234.333").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("lat", "-").field("x", 1.3).endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("lat", 1.3).field("y", "-").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "-,1.3").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "1.3,-").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("x", "NaN").field("y", "NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("lat", 12).field("y", "NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("x", "NaN").field("y", 10).endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "NaN,NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "10,NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "NaN,12").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject(FIELD_NAME).nullField("y").field("x", 1).endObject().endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(mapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject(FIELD_NAME).nullField("x").nullField("y").endObject().endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());
    }

    public void testZValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_z_value", true);
        }));

        ParsedDocument doc = mapper.parse(new SourceToParse("test1", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_NAME, "POINT (2000.1 305.6 34567.33)")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_z_value", false);
        }));

        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> mapper2.parse(new SourceToParse("test2", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field(FIELD_NAME, "POINT (2000.1 305.6 34567.33)")
                    .endObject()),
                XContentType.JSON))
        );
        assertThat(e.getMessage(), containsString("failed to parse field [" + FIELD_NAME + "] of type"));
        assertThat(e.getRootCause().getMessage(),
            containsString("found Z value [34567.33] but [ignore_z_value] parameter is [false]"));
    }
}
