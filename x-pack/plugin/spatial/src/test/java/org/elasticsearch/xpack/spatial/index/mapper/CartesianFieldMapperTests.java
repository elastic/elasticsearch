/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/** Base class for testing cartesian field mappers */
public abstract class CartesianFieldMapperTests extends MapperTestCase {

    static final String FIELD_NAME = "field";

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new LocalStateSpatialPlugin());
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {}

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", getFieldName());
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "POINT (14.0 15.0)";
    }

    protected abstract String getFieldName();

    protected abstract void assertXYPointField(IndexableField field, float x, float y);

    protected abstract void assertGeoJSONParseException(DocumentParsingException e, String missingField);

    public void testWKT() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field(FIELD_NAME, "POINT (2000.1 305.6)")));
        assertXYPointField(doc.rootDoc().getField(FIELD_NAME), 2000.1f, 305.6f);
    }

    public void testGeoJSONMissingCoordinates() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("type", "Point").endObject()))
        );
        assertGeoJSONParseException(e, "coordinates");
    }

    public void testGeoJSONMissingType() throws IOException {
        double[] coords = new double[] { 0.0, 0.0 };
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("coordinates", coords).endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("Required [type]"));
    }

    public void testGeoJSON() throws IOException {
        double[] coords = new double[] { 2000.1, 305.6 };
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject(FIELD_NAME).field("coordinates", coords).field("type", "Point").endObject())
        );
        assertXYPointField(doc.rootDoc().getField(FIELD_NAME), 2000.1f, 305.6f);
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    public void testZValueWKT() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_z_value", true);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field(FIELD_NAME, "POINT (2000.1 305.6 34567.33)")));

        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_z_value", false);
        }));

        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper2.parse(source(b -> b.field(FIELD_NAME, "POINT (2000.1 305.6 34567.33)")))
        );
        assertThat(e.getMessage(), containsString("failed to parse field [" + FIELD_NAME + "] of type"));
        assertThat(e.getRootCause().getMessage(), containsString("found Z value [34567.33] but [ignore_z_value] parameter is [false]"));
    }

    public void testZValueGeoJSON() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_z_value", true);
        }));

        double[] coords = { 2000.1, 305.6, 34567.33 };
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject(FIELD_NAME).field("type", "Point").field("coordinates", coords).endObject())
        );

        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_z_value", false);
        }));

        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper2.parse(source(b -> b.startObject(FIELD_NAME).field("type", "Point").field("coordinates", coords).endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse field [" + FIELD_NAME + "] of type"));
        assertThat(e.getCause().getMessage(), containsString("found Z value [34567.33] but [ignore_z_value] parameter is [false]"));
    }
}
