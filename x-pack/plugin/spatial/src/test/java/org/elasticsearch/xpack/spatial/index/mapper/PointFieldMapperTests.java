/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.hamcrest.CoreMatchers;

import java.util.Collection;

import static org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper.Names.NULL_VALUE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PointFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, SpatialPlugin.class, LocalStateCompositeXPackPlugin.class);
    }


    public void testWKT() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("point", "POINT (2000.1 305.6)")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testValuesStored() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("point").field("x", 2000.1).field("y", 305.6).endObject()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testArrayValues() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point").field("doc_values", false);
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point")
                        .startObject().field("x", 1.2).field("y", 1.3).endObject()
                        .startObject().field("x", 1.4).field("y", 1.5).endObject()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("point"), notNullValue());
        assertThat(doc.rootDoc().getFields("point").length, equalTo(4));
    }

    public void testLatLonInOneValue() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("point", "1.2,1.3")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testInOneValueStored() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("point", "1.2,1.3")
                        .endObject()),
                XContentType.JSON));
        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testLatLonInOneValueArray() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point").field("doc_values", false);
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point")
                        .value("1.2,1.3")
                        .value("1.4,1.5")
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("point"), notNullValue());
        assertThat(doc.rootDoc().getFields("point").length, equalTo(4));
    }

    public void testArray() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point").value(1.3).value(1.2).endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testArrayDynamic() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("point").field("match", "point*")
            .startObject("mapping").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endArray().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point").value(1.3).value(1.2).endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testArrayStored() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.field("store", true).endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point").value(1.3).value(1.2).endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("point"), notNullValue());
        assertThat(doc.rootDoc().getFields("point").length, equalTo(3));
    }

    public void testArrayArrayStored() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("point").field("type", "point");
        String mapping = Strings.toString(xContentBuilder.field("store", true)
            .field("doc_values", false).endObject().endObject()
            .endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("point")
                        .startArray().value(1.3).value(1.2).endArray()
                        .startArray().value(1.5).value(1.4).endArray()
                        .endArray()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getFields("point"), notNullValue());
        assertThat(doc.rootDoc().getFields("point").length, CoreMatchers.equalTo(4));
    }

    public void testEmptyName() throws Exception {
        // after 5.x
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("").field("type", "point").endObject().endObject()
            .endObject().endObject());

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testNullValue() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("location")
            .field("type", "point")
            .field(NULL_VALUE, "1,2")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(PointFieldMapper.class));

        Object nullValue = ((PointFieldMapper) fieldMapper).fieldType().nullValue();
        assertThat(nullValue, equalTo(new CartesianPoint(1, 2)));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .nullField("location")
                    .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("location"), notNullValue());
        BytesRef defaultValue = doc.rootDoc().getField("location").binaryValue();

        doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("location", "1, 2")
                    .endObject()),
            XContentType.JSON));
        // Shouldn't matter if we specify the value explicitly or use null value
        assertThat(defaultValue, equalTo(doc.rootDoc().getField("location").binaryValue()));

        doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("location", "3, 4")
                    .endObject()),
            XContentType.JSON));
        // Shouldn't matter if we specify the value explicitly or use null value
        assertThat(defaultValue, not(equalTo(doc.rootDoc().getField("location").binaryValue())));
    }

    public void testInvalidPointValuesIgnored() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("location")
            .field("type", "point")
            .field("ignore_malformed", "true")
            .endObject()
            .endObject().endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("location", "1234.333").endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("lat", "-").field("x", 1.3).endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("lat", 1.3).field("y", "-").endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("location", "-,1.3").endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("location", "1.3,-").endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("x", "NaN").field("y", "NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("lat", 12).field("y", "NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("x", "NaN").field("y", 10).endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("location", "NaN,NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("location", "10,NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("location", "NaN,12").endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("location").nullField("y").field("x", 1).endObject().endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("location").nullField("x").nullField("y").endObject().endObject()
            ), XContentType.JSON)).rootDoc().getField("location"), nullValue());
    }
}
