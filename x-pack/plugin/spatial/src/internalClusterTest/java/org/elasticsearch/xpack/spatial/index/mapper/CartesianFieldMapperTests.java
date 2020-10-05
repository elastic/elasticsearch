/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Base class for testing cartesian field mappers */
public abstract class CartesianFieldMapperTests  extends ESSingleNodeTestCase {

    private static final String FIELD_NAME = "location";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, LocalStateSpatialPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    protected abstract XContentBuilder createDefaultMapping(String fieldName,
                                                            boolean ignored_malformed,
                                                            boolean ignoreZValue) throws IOException;


    public void testWKT() throws IOException {
        String mapping = Strings.toString(createDefaultMapping(FIELD_NAME, randomBoolean(), randomBoolean()));
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_NAME, "POINT (2000.1 305.6)")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
    }

    public void testEmptyName() throws IOException {
        String mapping = Strings.toString(createDefaultMapping("", randomBoolean(), randomBoolean()));

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testInvalidPointValuesIgnored() throws IOException {
        String mapping = Strings.toString(createDefaultMapping(FIELD_NAME, true, randomBoolean()));

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "1234.333").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("lat", "-").field("x", 1.3).endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("lat", 1.3).field("y", "-").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "-,1.3").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "1.3,-").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("x", "NaN").field("y", "NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("lat", 12).field("y", "NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("x", "NaN").field("y", 10).endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "NaN,NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "10,NaN").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field(FIELD_NAME, "NaN,12").endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject(FIELD_NAME).nullField("y").field("x", 1).endObject().endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());

        assertThat(defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject(FIELD_NAME).nullField("x").nullField("y").endObject().endObject()
            ), XContentType.JSON)).rootDoc().getField(FIELD_NAME), nullValue());
    }

    public void testZValue() throws IOException {
        String mapping = Strings.toString(createDefaultMapping(FIELD_NAME, false, true));
        DocumentMapper defaultMapper = createIndex("test1").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test1", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_NAME, "POINT (2000.1 305.6 34567.33)")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());

        mapping = Strings.toString(createDefaultMapping(FIELD_NAME, false, false));
        DocumentMapper defaultMapper2 = createIndex("test2").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> defaultMapper2.parse(new SourceToParse("test2", "1",
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
