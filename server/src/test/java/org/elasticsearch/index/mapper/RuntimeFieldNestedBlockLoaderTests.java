/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
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

    private static final int[] PARENT_DOCS = { 2, 4, 6 };

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

    public void testParentDocumentsReadTheValuesOfTheRuntimeFieldUnderTheNestedFieldWithSyntheticSource() throws IOException {
        assertThat(readParentValues("items.calc", true), contains(Arrays.asList(10L, 20L), null, 30L));
    }

    public void testParentDocumentsReadTheValuesOfTheRuntimeFieldUnderTheNestedFieldWithStoredSource() throws IOException {
        assertThat(readParentValues("items.calc", false), contains(Arrays.asList(10L, 20L), null, 30L));
    }

    private List<Object> readParentValues(String field, boolean synthetic) throws IOException {
        Settings settings = synthetic ? Settings.builder().put("index.mapping.source.mode", "synthetic").build() : Settings.EMPTY;
        MapperService mapperService = createMapperService(settings, MAPPING);
        List<Object> values = new ArrayList<>();
        withLuceneIndex(mapperService, writer -> {
            writer.addDocuments(parse(mapperService, """
                {"id":"a","items":[{"qty":1,"calc":10},{"qty":2,"calc":20}]}""").docs());
            writer.addDocuments(parse(mapperService, """
                {"id":"b","items":[{"qty":3}]}""").docs());
            writer.addDocuments(parse(mapperService, """
                {"id":"c","items":[{"qty":4,"calc":30}]}""").docs());
            writer.forceMerge(1);
        }, unwrapped -> {
            DirectoryReader reader = ElasticsearchDirectoryReader.wrap(unwrapped, new ShardId("index", "_na_", 0));
            SearchLookup lookup = createSearchExecutionContext(mapperService, new IndexSearcher(reader)).lookup();
            var context = new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService) {
                @Override
                public SearchLookup lookup() {
                    return lookup;
                }
            };
            BlockLoader loader = mapperService.fieldType(field).blockLoader(context);
            LeafReaderContext leaf = reader.leaves().get(0);
            try (
                BlockLoader.RowStrideReader rowReader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), leaf);
                BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 3)
            ) {
                BlockLoaderStoredFieldsFromLeafLoader storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                    StoredFieldLoader.fromSpec(loader.rowStrideStoredFieldSpec()).getLoader(leaf, null),
                    null
                );
                for (int parentDoc : PARENT_DOCS) {
                    storedFields.advanceTo(parentDoc);
                    rowReader.read(parentDoc, storedFields, builder);
                }
                TestBlock block = (TestBlock) builder.build();
                for (int i = 0; i < block.size(); i++) {
                    values.add(block.get(i));
                }
            }
        });
        return values;
    }

    private static ParsedDocument parse(MapperService mapperService, String json) {
        return mapperService.documentParser().parseDocument(source(json), mapperService.mappingLookup());
    }

}
