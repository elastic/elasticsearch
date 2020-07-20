/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.datastreams.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.MapperTestUtils.assertConflicts;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamTimestampFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    public void testPostParse() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("_data_stream_timestamp")
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject("@timestamp")
                .field("type", randomBoolean() ? "date" : "date_nanos")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        DocumentMapper docMapper = createIndex("test").mapperService()
            .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("@timestamp", "2020-12-12").endObject()),
                XContentType.JSON
            )
        );
        assertThat(doc.rootDoc().getFields("@timestamp").length, equalTo(2));

        Exception e = expectThrows(
            MapperException.class,
            () -> docMapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("@timestamp1", "2020-12-12").endObject()),
                    XContentType.JSON
                )
            )
        );
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] is missing"));

        e = expectThrows(
            MapperException.class,
            () -> docMapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder().startObject().array("@timestamp", "2020-12-12", "2020-12-13").endObject()
                    ),
                    XContentType.JSON
                )
            )
        );
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] encountered multiple values"));
    }

    public void testValidateNonExistingField() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("_data_stream_timestamp")
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject("my_date_field")
                .field("type", "date")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex("test").mapperService()
                .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] does not exist"));
    }

    public void testValidateInvalidFieldType() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("_data_stream_timestamp")
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject("@timestamp")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex("test").mapperService()
                .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] is of type [keyword], but [date,date_nanos] is expected")
        );
    }

    public void testValidateNotIndexed() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("_data_stream_timestamp")
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject("@timestamp")
                .field("type", "date")
                .field("index", "false")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex("test").mapperService()
                .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] is not indexed"));
    }

    public void testValidateNotDocValues() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("_data_stream_timestamp")
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject("@timestamp")
                .field("type", "date")
                .field("doc_values", "false")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex("test").mapperService()
                .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] doesn't have doc values"));
    }

    public void testValidateNullValue() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("_data_stream_timestamp")
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject("@timestamp")
                .field("type", "date")
                .field("null_value", "2020-12-12")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex("test").mapperService()
                .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] has disallowed [null_value] attribute specified"));
    }

    public void testValidateIgnoreMalformed() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("_data_stream_timestamp")
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject("@timestamp")
                .field("type", "date")
                .field("ignore_malformed", "true")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex("test").mapperService()
                .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] has disallowed [ignore_malformed] attribute specified")
        );
    }

    public void testValidateNotDisallowedAttribute() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("_data_stream_timestamp")
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject("@timestamp")
                .field("type", "date")
                .field("store", "true")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex("test").mapperService()
                .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] has disallowed attributes: [store]"));
    }

    public void testCannotUpdateTimestampField() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping1 =
            "{\"type\":{\"_data_stream_timestamp\":{\"enabled\":false}, \"properties\": {\"@timestamp\": {\"type\": \"date\"}}}}}";
        String mapping2 = "{\"type\":{\"_data_stream_timestamp\":{\"enabled\":true}, \"properties\": {\"@timestamp2\": "
            + "{\"type\": \"date\"},\"@timestamp\": {\"type\": \"date\"}}}})";
        assertConflicts(mapping1, mapping2, parser, "Mapper for [_data_stream_timestamp]", "[enabled] from [false] to [true]");

        mapping1 = "{\"type\":{\"properties\":{\"@timestamp\": {\"type\": \"date\"}}}}}";
        mapping2 = "{\"type\":{\"_data_stream_timestamp\":{\"enabled\":true}, \"properties\": "
            + "{\"@timestamp2\": {\"type\": \"date\"},\"@timestamp\": {\"type\": \"date\"}}}})";
        assertConflicts(mapping1, mapping2, parser, "Mapper for [_data_stream_timestamp]", "[enabled] from [false] to [true]");
    }

}
