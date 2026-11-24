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

public class SearchExecutionContextRuntimeLookupTests extends MapperServiceTestCase {

    private SearchExecutionContext context() throws IOException {
        return createSearchExecutionContext(createMapperService("""
            { "_doc" : {
              "runtime" : { "calc" : { "type" : "long" }, "items.calc" : { "type" : "long" }, "obj.calc" : { "type" : "long" } },
              "properties" : {
                "id" : { "type" : "keyword" },
                "obj" : { "properties" : { "v" : { "type" : "long" } } },
                "items" : { "type" : "nested", "dynamic" : false, "properties" : { "qty" : { "type" : "long" } } }
              }
            } }
            """));
    }

    public void testARuntimeFieldAtTheRootIsOnlyMappedAsARuntimeField() throws IOException {
        assertThat(context().lookup().onlyMappedAsRuntimeField("calc"), is(true));
    }

    public void testARuntimeFieldUnderAnObjectIsOnlyMappedAsARuntimeField() throws IOException {
        assertThat(context().lookup().onlyMappedAsRuntimeField("obj.calc"), is(true));
    }

    public void testAMappedFieldIsNotOnlyMappedAsARuntimeField() throws IOException {
        assertThat(context().lookup().onlyMappedAsRuntimeField("id"), is(false));
        assertThat(context().lookup().onlyMappedAsRuntimeField("obj.v"), is(false));
    }

    public void testARuntimeFieldUnderANestedFieldIsNotOnlyMappedAsARuntimeField() throws IOException {
        assertThat(context().lookup().onlyMappedAsRuntimeField("items.calc"), is(false));
    }
}
