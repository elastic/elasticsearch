/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

public class TimestampMetadataFieldMapperTests extends MapperServiceTestCase {

    @Override
    protected Settings getIndexSettings() {
        return Settings.builder().put("index.mode", "time_series").put("index.routing_path", "field").build();
    }

    public void testTimestampIndexing() throws IOException {
        DocumentMapper mapper = createDocumentMapper("""
            { "_doc" : { "properties" : { "field" : { "type" : "keyword", "time_series_dimension" : true } } } }
            """);

        ParsedDocument doc = mapper.parse(source("""
            { "field" : "value", "@timestamp" : "2022-02-22T22:22:22Z" }
            """));

        IndexableField[] fields = doc.rootDoc().getFields("@timestamp");
        assertEquals(2, fields.length);
    }

    public void testTimestampMustBePresent() throws IOException {
        DocumentMapper mapper = createDocumentMapper("""
            { "_doc" : { "properties" : { "field" : { "type" : "keyword", "time_series_dimension" : true } } } }
            """);

        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source("""
            { "field" : "value" }
            """)));
        assertEquals("Document did not contain a @timestamp field", e.getMessage());
    }

    public void testTimestampMustBeSingleValued() throws IOException {
        DocumentMapper mapper = createDocumentMapper("""
            { "_doc" : { "properties" : { "field" : { "type" : "keyword", "time_series_dimension" : true } } } }
            """);

        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source("""
            { "field" : "value", "@timestamp" : [ "2022-02-22T22:22:22Z", "2022-02-22T22:22:23Z" ] }
            """)));
        assertEquals("@timestamp field can only have a single value", e.getMessage());
    }

    public void testMalformedTimestamp() throws IOException {
        DocumentMapper mapper = createDocumentMapper("""
            { "_doc" : { "properties" : { "field" : { "type" : "keyword", "time_series_dimension" : true } } } }
            """);

        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source("""
            { "field" : "value", "@timestamp" : "foo" }
            """)));
        assertEquals("failed to parse date field [foo] with format [strict_date_optional_time||epoch_millis]", e.getCause().getMessage());
    }
}
