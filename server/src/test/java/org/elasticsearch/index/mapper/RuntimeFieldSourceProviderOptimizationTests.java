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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that source provider optimization that filters _source based on the same of source only runtime fields kick in.
 * This is important for synthetic source, otherwise many doc value and stored fields get loaded in the process.
 */
public class RuntimeFieldSourceProviderOptimizationTests extends ESSingleNodeTestCase {

    public void testWithSourceProviderOptimization() throws IOException {
        var mapping = jsonBuilder().startObject().startObject("runtime").startObject("field");
        mapping.field("type", "long");
        mapping.endObject().endObject().endObject();
        var indexService = createIndex("test-index", Settings.builder().put("index.mapping.source.mode", "synthetic").build(), mapping);

        int numDocs = 256;
        try (Directory directory = newDirectory(); IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                BytesArray source = new BytesArray(String.format(Locale.ROOT, "{\"field\":%d,\"another_field\":123}", i));
                var doc = indexService.mapperService()
                    .documentMapper()
                    .parse(new SourceToParse(Integer.toString(i), source, XContentType.JSON))
                    .rootDoc();
                iw.addDocument(doc);
            }
            iw.commit();
            iw.forceMerge(1);

            try (var indexReader = DirectoryReader.open(iw)) {
                var searcher = new IndexSearcher(indexReader);
                LeafReaderContext leafReaderContext = indexReader.leaves().getFirst();
                var context = indexService.newSearchExecutionContext(0, 0, searcher, () -> 1L, null, Map.of());
                var fieldType = (AbstractScriptFieldType<?>) indexService.mapperService().fieldType("field");

                // The other_field should have been filtered out, otherwise the mechanism that pushes field name as source filter to
                // SourceProvider isn't kicking in. Essentially checking that optimization in
                // ConcurrentSegmentSourceProvider.optimizedSourceProvider(...) kicks in:
                var leafFactory = (LongFieldScript.LeafFactory) fieldType.leafFactory(context);
                var fieldScript = leafFactory.newInstance(leafReaderContext);
                for (int i = 0; i < 256; i++) {
                    fieldScript.runForDoc(i);
                    var source = fieldScript.source().get().source();
                    assertThat(source, equalTo(Map.of("field", i)));
                }

                // Test that runtime based term query works as expected with the optimization:
                var termQuery = fieldType.termQuery(32, context);
                assertThat(searcher.count(termQuery), equalTo(1));

                // Test that runtime based block loader works as expected with the optimization:
                var blockLoader = fieldType.blockLoader(blContext(context.lookup()));
                var columnReader = blockLoader.columnAtATimeReader(leafReaderContext);
                var block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(leafReaderContext), 0, false);
                for (int i = 0; i < block.size(); i++) {
                    assertThat(block.get(i), equalTo((long) i));
                }
            }
        }
    }

    public void testWithoutSourceProviderOptimization() throws IOException {
        var mapping = jsonBuilder().startObject().startObject("runtime").startObject("field");
        mapping.field("type", "long");
        mapping.endObject().endObject().endObject();
        var indexService = createIndex("test-index", Settings.EMPTY, mapping);

        int numDocs = 256;
        try (Directory directory = newDirectory(); IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                BytesArray source = new BytesArray(String.format(Locale.ROOT, "{\"field\":%d,\"another_field\":123}", i));
                var doc = indexService.mapperService()
                    .documentMapper()
                    .parse(new SourceToParse(Integer.toString(i), source, XContentType.JSON))
                    .rootDoc();
                iw.addDocument(doc);
            }
            iw.commit();
            iw.forceMerge(1);

            try (var indexReader = DirectoryReader.open(iw)) {
                var searcher = new IndexSearcher(indexReader);
                LeafReaderContext leafReaderContext = indexReader.leaves().getFirst();
                var context = indexService.newSearchExecutionContext(0, 0, searcher, () -> 1L, null, Map.of());
                var fieldType = (AbstractScriptFieldType<?>) indexService.mapperService().fieldType("field");

                var leafFactory = (LongFieldScript.LeafFactory) fieldType.leafFactory(context);
                var fieldScript = leafFactory.newInstance(leafReaderContext);
                for (int i = 0; i < 256; i++) {
                    fieldScript.runForDoc(i);
                    var source = fieldScript.source().get().source();
                    assertThat(source, equalTo(Map.of("field", i, "another_field", 123)));
                }

                // Test that runtime based term query works as expected with the optimization:
                var termQuery = fieldType.termQuery(32, context);
                assertThat(searcher.count(termQuery), equalTo(1));

                // Test that runtime based block loader works as expected with the optimization:
                var blockLoader = fieldType.blockLoader(blContext(context.lookup()));
                var columnReader = blockLoader.columnAtATimeReader(leafReaderContext);
                var block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(leafReaderContext), 0, false);
                for (int i = 0; i < block.size(); i++) {
                    assertThat(block.get(i), equalTo((long) i));
                }
            }
        }
    }

    static MappedFieldType.BlockLoaderContext blContext(SearchLookup lookup) {
        return new MappedFieldType.BlockLoaderContext() {
            @Override
            public String indexName() {
                throw new UnsupportedOperationException();
            }

            @Override
            public IndexSettings indexSettings() {
                throw new UnsupportedOperationException();
            }

            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return MappedFieldType.FieldExtractPreference.NONE;
            }

            @Override
            public SearchLookup lookup() {
                return lookup;
            }

            @Override
            public Set<String> sourcePaths(String name) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String parentField(String field) {
                throw new UnsupportedOperationException();
            }

            @Override
            public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                return FieldNamesFieldMapper.FieldNamesFieldType.get(true);
            }
        };
    }
}
