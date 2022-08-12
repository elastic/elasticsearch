/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataMapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DataStreamTimestampFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return DataStreamTimestampFieldMapper.NAME;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "enabled",
            timestampMapping(true, b -> b.startObject("@timestamp").field("type", "date").endObject()),
            timestampMapping(false, b -> b.startObject("@timestamp").field("type", "date").endObject())
        );
        checker.registerUpdateCheck(
            timestampMapping(false, b -> b.startObject("@timestamp").field("type", "date").endObject()),
            timestampMapping(true, b -> b.startObject("@timestamp").field("type", "date").endObject()),
            dm -> assertTrue(dm.metadataMapper(DataStreamTimestampFieldMapper.class).isEnabled())
        );
    }

    private XContentBuilder timestampMapping(boolean enabled, CheckedConsumer<XContentBuilder, IOException> propertiesBuilder)
        throws IOException {
        return topMapping(b -> {
            b.startObject(DataStreamTimestampFieldMapper.NAME).field("enabled", enabled).endObject();
            b.startObject("properties");
            propertiesBuilder.accept(b);
            b.endObject();
        });
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new DataStreamsPlugin());
    }

    public void testPostParse() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", randomBoolean() ? "date" : "date_nanos");
            b.endObject();
        }));

        ParsedDocument doc = docMapper.parse(source(b -> b.field("@timestamp", "2020-12-12")));
        assertThat(doc.rootDoc().getFields("@timestamp").length, equalTo(2));

        Exception e = expectThrows(MapperException.class, () -> docMapper.parse(source(b -> b.field("@timestamp1", "2020-12-12"))));
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] is missing"));

        e = expectThrows(MapperException.class, () -> docMapper.parse(source(b -> b.array("@timestamp", "2020-12-12", "2020-12-13"))));
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] encountered multiple values"));
    }

    public void testValidateNonExistingField() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(timestampMapping(true, b -> b.startObject("my_date_field").field("type", "date").endObject()))
        );
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] does not exist"));
    }

    public void testValidateInvalidFieldType() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(timestampMapping(true, b -> b.startObject("@timestamp").field("type", "keyword").endObject()))
        );
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] is of type [keyword], but [date,date_nanos] is expected")
        );
    }

    public void testValidateNotIndexed() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("index", false);
            b.endObject();
        })));
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] is not indexed"));
    }

    public void testValidateNotDocValues() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("doc_values", false);
            b.endObject();
        })));
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] doesn't have doc values"));
    }

    public void testValidateNullValue() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("null_value", "2020-12-12");
            b.endObject();
        })));
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] has disallowed [null_value] attribute specified"));
    }

    public void testValidateIgnoreMalformed() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("ignore_malformed", true);
            b.endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] has disallowed [ignore_malformed] attribute specified")
        );
    }

    public void testValidateNotDisallowedAttribute() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("store", true);
            b.endObject();
        })));
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] has disallowed attributes: [store]"));
    }

    public void testValidateDefaultIgnoreMalformed() throws Exception {
        Settings indexSettings = Settings.builder().put(FieldMapper.IGNORE_MALFORMED_SETTING.getKey(), true).build();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(Version.CURRENT, indexSettings, () -> true, timestampMapping(true, b -> {
                b.startObject("@timestamp");
                b.field("type", "date");
                b.endObject();
            }))
        );
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] has disallowed [ignore_malformed] attribute specified")
        );

        MapperService mapperService = createMapperService(Version.CURRENT, indexSettings, () -> true, timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("ignore_malformed", false);
            b.endObject();
        }));
        assertThat(mapperService, notNullValue());
        assertThat(mapperService.documentMapper().mappers().getMapper("@timestamp"), notNullValue());
        assertThat(((DateFieldMapper) mapperService.documentMapper().mappers().getMapper("@timestamp")).getIgnoreMalformed(), is(false));
    }
}
