/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class SearchExecutionContextIsRuntimeFieldTests extends MapperServiceTestCase {

    private SearchExecutionContext context() throws IOException {
        return createSearchExecutionContext(createMapperService("""
            { "_doc" : {
              "runtime" : {
                "calc" : { "type" : "long" },
                "items.calc" : { "type" : "long" },
                "obj.calc" : { "type" : "long" },
                "id" : { "type" : "keyword" },
                "lookup_field" : {
                  "type" : "lookup",
                  "target_index" : "other",
                  "input_field" : "id",
                  "target_field" : "id",
                  "fetch_fields" : [ "name" ]
                }
              },
              "properties" : {
                "id" : { "type" : "keyword" },
                "name" : { "type" : "keyword", "fields" : { "raw" : { "type" : "keyword" } } },
                "obj" : { "properties" : { "v" : { "type" : "long" } } },
                "items" : { "type" : "nested", "dynamic" : false, "properties" : { "qty" : { "type" : "long" } } },
                "flat" : { "type" : "flattened" }
              }
            } }
            """));
    }

    public void testARuntimeFieldAtTheRootIsARuntimeField() throws IOException {
        assertThat(context().isRuntimeField("calc"), is(true));
    }

    public void testARuntimeFieldUnderAnObjectIsARuntimeField() throws IOException {
        assertThat(context().isRuntimeField("obj.calc"), is(true));
    }

    public void testARuntimeFieldUnderANestedFieldIsARuntimeField() throws IOException {
        assertThat(context().isRuntimeField("items.calc"), is(true));
    }

    public void testALookupRuntimeFieldIsARuntimeField() throws IOException {
        assertThat(context().isRuntimeField("lookup_field"), is(true));
    }

    public void testARuntimeFieldThatShadowsAMappedFieldIsARuntimeField() throws IOException {
        assertThat(context().isRuntimeField("id"), is(true));
    }

    public void testAMappedFieldIsNotARuntimeField() throws IOException {
        SearchExecutionContext context = context();
        assertThat(context.isRuntimeField("name"), is(false));
        assertThat(context.isRuntimeField("name.raw"), is(false));
        assertThat(context.isRuntimeField("obj.v"), is(false));
        assertThat(context.isRuntimeField("items.qty"), is(false));
    }

    public void testAKeyOfAFlattenedFieldIsNotARuntimeField() throws IOException {
        assertThat(context().isRuntimeField("flat.key"), is(false));
    }

    public void testAnUnknownNameIsNotARuntimeField() throws IOException {
        assertThat(context().isRuntimeField("nothing.here"), is(false));
    }

    public void testNothingIsARuntimeFieldWithoutARuntimeSection() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext(createMapperService("""
            { "_doc" : { "properties" : { "id" : { "type" : "keyword" }, "flat" : { "type" : "flattened" } } } }
            """));
        assertThat(context.isRuntimeField("id"), is(false));
        assertThat(context.isRuntimeField("flat.key"), is(false));
    }
}
