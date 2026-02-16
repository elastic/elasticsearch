/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class FieldCountingTests extends MapperServiceTestCase {

    public void testNestedFieldPathWithSubobjectsTrue() throws Exception {
        assertFieldCount(true, b -> {
            b.startObject("host");
            b.startObject("properties");
            b.startObject("os");
            b.startObject("properties");
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }, 3L);
    }

    public void testNestedFieldPathWithSubobjectsFalse() throws Exception {
        assertFieldCount(false, b -> { b.startObject("host.os.name").field("type", "keyword").endObject(); }, 1L);
    }

    public void testMultiFields() throws Exception {
        assertFieldCount(true, b -> {
            b.startObject("title");
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.startObject("raw").field("type", "keyword").field("index", false).endObject();
            b.endObject();
            b.endObject();
        }, 3L);
    }

    public void testRuntimeFields() throws Exception {
        final MapperService mapperService = createMapperService(
            mapping(b -> { b.startObject("timestamp").field("type", "date").endObject(); })
        );
        assertThat(mapperService.mappingLookup().getTotalFieldsCount(), equalTo(1L));

        merge(mapperService, runtimeMapping(b -> { b.startObject("day_of_week").field("type", "keyword").endObject(); }));
        assertThat(mapperService.mappingLookup().getTotalFieldsCount(), equalTo(2L));
    }

    public void testFieldAliases() throws Exception {
        assertFieldCount(true, b -> {
            b.startObject("user_id").field("type", "keyword").endObject();
            b.startObject("user");
            b.field("type", "alias");
            b.field("path", "user_id");
            b.endObject();
        }, 2L);
    }

    public void testNestedObjectWithAliasWithSubobjectsTrue() throws Exception {
        assertFieldCount(true, b -> {
            b.startObject("host");
            b.startObject("properties");
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
            b.startObject("hostname");
            b.field("type", "alias");
            b.field("path", "host.name");
            b.endObject();
        }, 3L);
    }

    public void testNestedObjectWithAliasWithSubobjectsFalse() throws Exception {
        assertFieldCount(false, b -> {
            b.startObject("host.name").field("type", "keyword").endObject();
            b.startObject("hostname");
            b.field("type", "alias");
            b.field("path", "host.name");
            b.endObject();
        }, 2L);
    }

    private void assertFieldCount(
        final boolean subobjects,
        final CheckedConsumer<XContentBuilder, IOException> mapping,
        final long expectedCount
    ) throws Exception {
        final MapperService mapperService = createMapperService(subobjects ? mapping(mapping) : mappingNoSubobjects(mapping));
        assertThat(mapperService.mappingLookup().getTotalFieldsCount(), equalTo(expectedCount));
    }
}
