/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class RuntimeFieldNestedBlockLoaderTests extends MapperServiceTestCase {

    private static final String MAPPING = """
        { "_doc" : {
          "runtime" : { "calc" : { "type" : "long" }, "items.calc" : { "type" : "long" }, "obj.calc" : { "type" : "long" } },
          "properties" : {
            "id" : { "type" : "keyword" },
            "obj" : { "properties" : { "v" : { "type" : "long" } } },
            "items" : { "type" : "nested", "dynamic" : false, "properties" : { "qty" : { "type" : "long" } } }
          }
        } }
        """;

    private BlockLoader blockLoader(String name, boolean synthetic) throws IOException {
        Settings settings = synthetic ? Settings.builder().put("index.mapping.source.mode", "synthetic").build() : Settings.EMPTY;
        MapperService mapperService = createMapperService(settings, MAPPING);
        SearchLookup lookup = createSearchExecutionContext(mapperService).lookup();
        var context = new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService) {
            @Override
            public SearchLookup lookup() {
                return lookup;
            }
        };
        return mapperService.fieldType(name).blockLoader(context);
    }

    public void testARuntimeFieldAtTheRootReadsIgnoredSourceWithSyntheticSource() throws IOException {
        assertThat(blockLoader("calc", true), instanceOf(FallbackSyntheticSourceBlockLoader.class));
    }

    public void testARuntimeFieldUnderAnObjectReadsIgnoredSourceWithSyntheticSource() throws IOException {
        assertThat(blockLoader("obj.calc", true), instanceOf(FallbackSyntheticSourceBlockLoader.class));
    }

    public void testARuntimeFieldUnderANestedFieldReadsThroughItsScriptWithSyntheticSource() throws IOException {
        assertThat(blockLoader("items.calc", true), not(instanceOf(FallbackSyntheticSourceBlockLoader.class)));
    }

    public void testARuntimeFieldUnderANestedFieldReadsThroughItsScriptWithStoredSource() throws IOException {
        assertThat(blockLoader("items.calc", false), not(instanceOf(FallbackSyntheticSourceBlockLoader.class)));
    }

    public void testTheNestedConditionLeavesTheMeaningOfOnlyMappedAsRuntimeFieldAlone() throws IOException {
        MapperService mapperService = createMapperService(MAPPING);
        SearchLookup lookup = createSearchExecutionContext(mapperService).lookup();
        assertThat(lookup.onlyMappedAsRuntimeField("items.calc"), is(true));
    }
}
