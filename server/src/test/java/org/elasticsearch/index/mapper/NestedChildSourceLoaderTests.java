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
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class NestedChildSourceLoaderTests extends MapperServiceTestCase {

    private static final String MAPPING = """
        {
            "_doc": {
                "properties": {
                    "id": { "type": "keyword" },
                    "items": {
                        "type": "nested",
                        "properties": {
                            "v": { "type": "keyword" },
                            "sub": {
                                "type": "nested",
                                "properties": {
                                    "w": { "type": "keyword" }
                                }
                            }
                        }
                    },
                    "events": {
                        "type": "nested",
                        "properties": {
                            "s": { "type": "keyword" }
                        }
                    }
                }
            }
        }
        """;

    /**
     * Every child of a parent with many children gets its own slice when the children are read one after another, which
     * counts each child's offset from the child before it.
     */
    public void testManyChildrenInOrder() throws IOException {
        MapperService mapperService = createMapperService(MAPPING);
        int count = between(500, 2000);
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startArray("items");
            for (int i = 0; i < count; i++) {
                b.startObject().field("v", "v" + i).endObject();
            }
            b.endArray();
        }));
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            LeafReaderContext ctx = reader.leaves().getFirst();
            SourceLoader.Leaf leaf = childLoader(mapperService).leaf(ctx, null);
            LeafStoredFieldLoader storedFields = StoredFieldLoader.create(true, Set.of()).getLoader(ctx, null);
            for (int i = 0; i < count; i++) {
                storedFields.advanceTo(i);
                assertThat(leaf.source(storedFields, i).source(), equalTo(Map.of("items", Map.of("v", "v" + i))));
            }
        });
    }

    /**
     * Reading any increasing subset of documents, roots and children of any nested field at any depth, gives the slices
     * search's {@link NestedStoredSourceLoader} gives.
     */
    public void testMatchesNestedStoredSourceLoader() throws IOException {
        MapperService mapperService = createMapperService(MAPPING);
        withLuceneIndex(mapperService, iw -> {
            int roots = between(1, 20);
            for (int r = 0; r < roots; r++) {
                iw.addDocuments(mapperService.documentMapper().parse(source(b -> randomRoot(b))).docs());
            }
        }, reader -> {
            for (LeafReaderContext ctx : reader.leaves()) {
                List<Integer> docs = new ArrayList<>();
                for (int d = 0; d < ctx.reader().maxDoc(); d++) {
                    if (randomBoolean()) {
                        docs.add(d);
                    }
                }
                SourceLoader.Leaf expected = mapperService.mappingLookup()
                    .newSourceLoader(null, SourceFieldMetrics.NOOP, nestedDocuments(mapperService))
                    .leaf(ctx, null);
                SourceLoader.Leaf actual = childLoader(mapperService).leaf(ctx, null);
                LeafStoredFieldLoader expectedFields = StoredFieldLoader.create(true, Set.of()).getLoader(ctx, null);
                LeafStoredFieldLoader actualFields = StoredFieldLoader.create(true, Set.of()).getLoader(ctx, null);
                for (int d : docs) {
                    expectedFields.advanceTo(d);
                    actualFields.advanceTo(d);
                    Map<String, Object> want = expected.source(expectedFields, d).source();
                    assertThat("doc " + d, actual.source(actualFields, d).source(), equalTo(want));
                }
            }
        });
    }

    public void testChildOfNextParentIsNotReadFromPreviousParent() throws IOException {
        MapperService mapperService = createMapperService(MAPPING);
        assertChildSources(
            mapperService,
            List.of(
                parent("a", List.of(Map.of("v", "a0"), Map.of("v", "a1"))),
                parent("b", List.of(Map.of("v", "b0"), Map.of("v", "b1"), Map.of("v", "b2")))
            ),
            List.of(
                Map.of("items", Map.of("v", "a0")),
                Map.of("items", Map.of("v", "a1")),
                Map.of("items", Map.of("v", "b0")),
                Map.of("items", Map.of("v", "b1")),
                Map.of("items", Map.of("v", "b2"))
            )
        );
    }

    public void testSyntheticSourceChildrenOfSeveralParents() throws IOException {
        MapperService mapperService = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("id").field("type", "keyword").endObject();
            b.startObject("items").field("type", "nested").startObject("properties");
            {
                b.startObject("v").field("type", "keyword").endObject();
                b.startObject("n").field("type", "long").endObject();
                b.startObject("tags").field("type", "keyword").endObject();
            }
            b.endObject().endObject();
        }));
        assertChildSources(
            mapperService,
            List.of(
                parent("a", List.of(Map.of("v", "a0", "n", 1, "tags", List.of("x", "y")), Map.of("v", "a1", "n", 2))),
                parent("empty", List.of()),
                parent("b", List.of(Map.of("v", "b0", "n", 10), Map.of("v", "b1", "n", 20, "tags", List.of("z")), Map.of("v", "b2")))
            ),
            List.of(
                Map.of("items", Map.of("v", "a0", "n", 1, "tags", List.of("x", "y"))),
                Map.of("items", Map.of("v", "a1", "n", 2)),
                Map.of("items", Map.of("v", "b0", "n", 10)),
                Map.of("items", Map.of("v", "b1", "n", 20, "tags", "z")),
                Map.of("items", Map.of("v", "b2"))
            )
        );
    }

    public void testSyntheticSourceChildrenWithFallbackFields() throws IOException {
        MapperService mapperService = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("id").field("type", "keyword").endObject();
            b.startObject("items").field("type", "nested").startObject("properties");
            {
                b.startObject("v").field("type", "keyword").endObject();
                b.startObject("capped").field("type", "keyword").field("ignore_above", 3).endObject();
                b.startObject("blob").field("type", "object").field("enabled", false).endObject();
            }
            b.endObject().endObject();
        }));
        assertChildSources(
            mapperService,
            List.of(
                parent(
                    "a",
                    List.of(Map.of("v", "a0", "capped", "a-long-value", "blob", Map.of("k", "a0")), Map.of("v", "a1", "capped", "ok"))
                ),
                parent("b", List.of(Map.of("v", "b0", "blob", Map.of("k", "b0", "list", List.of(1, 2))), Map.of("capped", "b-long-value")))
            ),
            List.of(
                Map.of("items", Map.of("v", "a0", "capped", "a-long-value", "blob", Map.of("k", "a0"))),
                Map.of("items", Map.of("v", "a1", "capped", "ok")),
                Map.of("items", Map.of("v", "b0", "blob", Map.of("k", "b0", "list", List.of(1, 2)))),
                Map.of("items", Map.of("capped", "b-long-value"))
            )
        );
    }

    public void testSyntheticSourceKeepAllChildren() throws IOException {
        MapperService mapperService = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("id").field("type", "keyword").endObject();
            b.startObject("items").field("type", "nested").field("synthetic_source_keep", "all").startObject("properties");
            {
                b.startObject("v").field("type", "keyword").endObject();
                b.startObject("tags").field("type", "keyword").endObject();
            }
            b.endObject().endObject();
        }));
        assertChildSources(
            mapperService,
            List.of(
                parent("a", List.of(Map.of("v", "a0", "tags", List.of("b", "a", "b")), Map.of("v", "a1", "extra", 7))),
                parent("b", List.of(Map.of("v", "b0", "tags", List.of("z", "y")), Map.of("v", "b1")))
            ),
            List.of(
                Map.of("items", Map.of("v", "a0", "tags", List.of("b", "a", "b"))),
                Map.of("items", Map.of("v", "a1", "extra", 7)),
                Map.of("items", Map.of("v", "b0", "tags", List.of("z", "y"))),
                Map.of("items", Map.of("v", "b1"))
            )
        );
    }

    public void testRootBytesHeldIsComputedOncePerRoot() throws IOException {
        MapperService mapperService = createMapperService(MAPPING);
        List<LuceneDocument> docs = new ArrayList<>();
        docs.addAll(
            mapperService.documentMapper()
                .parse(source(parent("a", List.of(Map.of("v", "a0"), Map.of("v", "a1"), Map.of("v", "a2")))))
                .docs()
        );
        docs.addAll(mapperService.documentMapper().parse(source(parent("b", List.of(Map.of("v", "b0"), Map.of("v", "b1"))))).docs());
        withLuceneIndex(mapperService, iw -> iw.addDocuments(docs), reader -> {
            DirectoryReader shardReader = wrapInMockESDirectoryReader(reader);
            LeafReaderContext ctx = shardReader.leaves().getFirst();
            MappingLookup lookup = mapperService.mappingLookup();
            CountingBytesHeldSourceLoader rootLoader = new CountingBytesHeldSourceLoader(
                lookup.newSourceLoader(null, SourceFieldMetrics.NOOP, null)
            );
            SourceLoader loader = new NestedChildSourceLoader(rootLoader, nestedDocuments(mapperService), lookup.nestedLookup());
            SourceLoader.Leaf leaf = loader.leaf(ctx, null);
            LeafStoredFieldLoader storedFields = StoredFieldLoader.create(true, loader.requiredStoredFields()).getLoader(ctx, null);
            LeafStoredFieldLoader rootFields = StoredFieldLoader.create(true, Set.of()).getLoader(ctx, null);
            rootFields.advanceTo(3);
            long rootABytes = rootFields.source().length();
            rootFields.advanceTo(6);
            long rootBBytes = rootFields.source().length();
            List<Long> bytesHeld = new ArrayList<>();
            for (int docId : new int[] { 0, 1, 2, 4, 5 }) {
                storedFields.advanceTo(docId);
                bytesHeld.add(leaf.sourceBytesHeld(leaf.source(storedFields, docId)));
            }
            assertThat(bytesHeld, equalTo(List.of(rootABytes, rootABytes, rootABytes, rootBBytes, rootBBytes)));
            assertThat(rootLoader.sourceBytesHeldCalls.get(), equalTo(2));
        });
    }

    private static class CountingBytesHeldSourceLoader implements SourceLoader {
        private final SourceLoader delegate;
        private final AtomicInteger sourceBytesHeldCalls = new AtomicInteger();

        CountingBytesHeldSourceLoader(SourceLoader delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean reordersFieldValues() {
            return delegate.reordersFieldValues();
        }

        @Override
        public Set<String> requiredStoredFields() {
            return delegate.requiredStoredFields();
        }

        @Override
        public Leaf leaf(LeafReaderContext ctx, int[] docIdsInLeaf) throws IOException {
            Leaf leaf = delegate.leaf(ctx, docIdsInLeaf);
            return new Leaf() {
                @Override
                public Source source(LeafStoredFieldLoader storedFields, int docId) throws IOException {
                    return leaf.source(storedFields, docId);
                }

                @Override
                public void write(LeafStoredFieldLoader storedFields, int docId, XContentBuilder b) throws IOException {
                    leaf.write(storedFields, docId, b);
                }

                @Override
                public long sourceBytesHeld(Source source) {
                    sourceBytesHeldCalls.incrementAndGet();
                    return leaf.sourceBytesHeld(source);
                }
            };
        }
    }

    public void testRefusesStoredSourceFilteredByMapping() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("_source").array(randomFrom("includes", "excludes"), "id", "items.v").endObject();
            b.startObject("properties");
            {
                b.startObject("id").field("type", "keyword").endObject();
                b.startObject("items").field("type", "nested").startObject("properties");
                b.startObject("v").field("type", "keyword").endObject();
                b.endObject().endObject();
            }
            b.endObject();
        }));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> childLoader(mapperService));
        assertThat(e.getMessage(), containsString("filtered _source"));
    }

    public void testFilteredStoredSourceWithoutNestedFieldsIsNotRefused() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("_source").array("excludes", "v").endObject();
            b.startObject("properties").startObject("v").field("type", "keyword").endObject().endObject();
        }));
        assertThat(childLoader(mapperService), notNullValue());
    }

    private static CheckedConsumer<XContentBuilder, IOException> parent(String id, List<Map<String, Object>> items) {
        return b -> b.field("id", id).field("items", items);
    }

    private void assertChildSources(
        MapperService mapperService,
        List<CheckedConsumer<XContentBuilder, IOException>> parents,
        List<Map<String, Object>> expectedChildSources
    ) throws IOException {
        List<LuceneDocument> docs = new ArrayList<>();
        List<Integer> childDocIds = new ArrayList<>();
        for (CheckedConsumer<XContentBuilder, IOException> parent : parents) {
            List<LuceneDocument> parsed = mapperService.documentMapper().parse(source(parent)).docs();
            for (int i = 0; i < parsed.size() - 1; i++) {
                childDocIds.add(docs.size() + i);
            }
            docs.addAll(parsed);
        }
        withLuceneIndex(mapperService, iw -> iw.addDocuments(docs), reader -> {
            DirectoryReader shardReader = wrapInMockESDirectoryReader(reader);
            assertThat(shardReader.leaves(), hasSize(1));
            LeafReaderContext ctx = shardReader.leaves().getFirst();
            SourceLoader loader = childLoader(mapperService);
            SourceLoader.Leaf leaf = loader.leaf(ctx, null);
            LeafStoredFieldLoader storedFields = StoredFieldLoader.create(false, loader.requiredStoredFields()).getLoader(ctx, null);
            List<Map<String, Object>> actual = new ArrayList<>();
            for (int docId : childDocIds) {
                storedFields.advanceTo(docId);
                actual.add(leaf.source(storedFields, docId).source());
            }
            assertThat(actual, equalTo(expectedChildSources));
        });
    }

    private static void randomRoot(XContentBuilder b) throws IOException {
        b.field("id", randomAlphaOfLength(4));
        if (randomBoolean()) {
            b.startArray("items");
            for (int i = between(0, 30); i > 0; i--) {
                b.startObject().field("v", randomAlphaOfLength(4));
                if (randomBoolean()) {
                    b.startArray("sub");
                    for (int s = between(0, 3); s > 0; s--) {
                        b.startObject().field("w", randomAlphaOfLength(4)).endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        }
        if (randomBoolean()) {
            b.startArray("events");
            for (int e = between(0, 10); e > 0; e--) {
                b.startObject().field("s", randomAlphaOfLength(4)).endObject();
            }
            b.endArray();
        }
    }

    private static SourceLoader childLoader(MapperService mapperService) {
        return mapperService.mappingLookup().newNestedChildSourceLoader(nestedDocuments(mapperService));
    }

    private static NestedDocuments nestedDocuments(MapperService mapperService) {
        return new NestedDocuments(mapperService.mappingLookup(), QueryBitSetProducer::new, IndexVersion.current());
    }
}
