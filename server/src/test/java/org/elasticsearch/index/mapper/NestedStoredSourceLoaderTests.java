/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class NestedStoredSourceLoaderTests extends MapperServiceTestCase {

    private static final String NESTED_MAPPING = """
        {
            "_doc": {
                "properties": {
                    "name": { "type": "keyword" },
                    "children": {
                        "type": "nested",
                        "properties": {
                            "value": { "type": "keyword" }
                        }
                    }
                }
            }
        }
        """;

    private static final String MULTILEVEL_MAPPING = """
        {
            "_doc": {
                "properties": {
                    "children": {
                        "type": "nested",
                        "properties": {
                            "name": { "type": "keyword" },
                            "grandchildren": {
                                "type": "nested",
                                "properties": {
                                    "name": { "type": "keyword" }
                                }
                            }
                        }
                    }
                }
            }
        }
        """;

    /**
     * A nested child (lower doc ID) is visited before its root parent (higher doc ID), which is
     * the normal forward-scan order. The child yields its nested slice; the root parent returns the
     * full source from the cache populated by the child visit.
     */
    public void testNestedChildThenRoot() throws IOException {
        MapperService mapperService = createMapperService(NESTED_MAPPING);

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "root");
            b.startArray("children");
            b.startObject().field("value", "child1").endObject();
            b.endArray();
        }));

        // doc 0 = nested child, doc 1 = root
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            SourceLoader loader = nestedLoader(mapperService);
            SourceLoader.Leaf leaf = loader.leaf(reader.leaves().get(0), null);

            LeafStoredFieldLoader leafLoader = storedFieldLoader(loader).getLoader(reader.leaves().get(0), null);

            leafLoader.advanceTo(0);
            Source childSource = leaf.source(leafLoader, 0);
            // extractSource wraps the slice in the nested path: {"children": {"value": "child1"}}
            @SuppressWarnings("unchecked")
            Map<String, Object> childrenMap = (Map<String, Object>) childSource.source().get("children");
            assertEquals("child1", childrenMap.get("value"));
            assertNull(childSource.source().get("name"));

            // Root visited after its child returns the full source (from cache set by the child visit).
            leafLoader.advanceTo(1);
            Source rootSource = leaf.source(leafLoader, 1);
            assertEquals("root", rootSource.source().get("name"));
            assertNotNull(rootSource.source().get("children"));
        });
    }

    /**
     * Multiple children of the same parent all receive distinct slices, and the root source is
     * loaded only once (for the first child). Subsequent children and the root itself hit the cache.
     * Caching is verified by counting calls to StoredFields.document() on the underlying reader.
     */
    public void testMultipleChildrenShareCachedRootSource() throws IOException {
        MapperService mapperService = createMapperService(NESTED_MAPPING);

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "root");
            b.startArray("children");
            b.startObject().field("value", "alpha").endObject();
            b.startObject().field("value", "beta").endObject();
            b.startObject().field("value", "gamma").endObject();
            b.endArray();
        }));

        // docs 0-2 = children (alpha, beta, gamma), doc 3 = root
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            AtomicInteger storedFieldReads = new AtomicInteger(0);
            LeafReader countingReader = countingLeafReader(reader.leaves().get(0).reader(), storedFieldReads);

            SourceLoader loader = nestedLoader(mapperService);
            SourceLoader.Leaf leaf = loader.leaf(countingReader.getContext(), null);

            LeafStoredFieldLoader leafLoader = storedFieldLoader(loader).getLoader(reader.leaves().get(0), null);
            List<String> expected = List.of("alpha", "beta", "gamma");
            for (int i = 0; i < 3; i++) {
                leafLoader.advanceTo(i);
                Source source = leaf.source(leafLoader, i);
                @SuppressWarnings("unchecked")
                Map<String, Object> childrenMap = (Map<String, Object>) source.source().get("children");
                assertEquals(expected.get(i), childrenMap.get("value"));
            }

            // Root accessed after children still returns the full source.
            leafLoader.advanceTo(3);
            Source rootSource = leaf.source(leafLoader, 3);
            assertEquals("root", rootSource.source().get("name"));

            // The internal leafRootLoader loaded the root doc once (for child 0); the caller's
            // leafLoader was advanced to children 0-2 (no _source) and then to root (cache hit).
            // So the counting reader's storedFields().document() was called exactly once.
            assertEquals(1, storedFieldReads.get());
        });
    }

    /**
     * Grandchildren in a two-level nesting hierarchy receive their correct nested slices. All three
     * nested docs (two grandchildren and the intermediate child) share a single root source load.
     */
    public void testMultiLevelNesting() throws IOException {
        MapperService mapperService = createMapperService(MULTILEVEL_MAPPING);

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startArray("children");
            b.startObject();
            b.field("name", "child1");
            b.startArray("grandchildren");
            b.startObject().field("name", "gc1").endObject();
            b.startObject().field("name", "gc2").endObject();
            b.endArray();
            b.endObject();
            b.endArray();
        }));

        // doc 0 = gc1, doc 1 = gc2, doc 2 = child1, doc 3 = root
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            AtomicInteger storedFieldReads = new AtomicInteger(0);
            LeafReader countingReader = countingLeafReader(reader.leaves().get(0).reader(), storedFieldReads);

            SourceLoader loader = nestedLoader(mapperService);
            SourceLoader.Leaf leaf = loader.leaf(countingReader.getContext(), null);

            LeafStoredFieldLoader leafLoader = storedFieldLoader(loader).getLoader(reader.leaves().get(0), null);

            // gc1: {"children": {"grandchildren": {"name": "gc1"}}}
            leafLoader.advanceTo(0);
            Source gc1Source = leaf.source(leafLoader, 0);
            @SuppressWarnings("unchecked")
            Map<String, Object> cm0 = (Map<String, Object>) gc1Source.source().get("children");
            @SuppressWarnings("unchecked")
            Map<String, Object> gcm0 = (Map<String, Object>) cm0.get("grandchildren");
            assertEquals("gc1", gcm0.get("name"));

            // gc2: {"children": {"grandchildren": {"name": "gc2"}}}
            leafLoader.advanceTo(1);
            Source gc2Source = leaf.source(leafLoader, 1);
            @SuppressWarnings("unchecked")
            Map<String, Object> cm1 = (Map<String, Object>) gc2Source.source().get("children");
            @SuppressWarnings("unchecked")
            Map<String, Object> gcm1 = (Map<String, Object>) cm1.get("grandchildren");
            assertEquals("gc2", gcm1.get("name"));

            // child1 slice includes its grandchildren array
            leafLoader.advanceTo(2);
            Source childSource = leaf.source(leafLoader, 2);
            @SuppressWarnings("unchecked")
            Map<String, Object> childSlice = (Map<String, Object>) childSource.source().get("children");
            assertEquals("child1", childSlice.get("name"));
            assertNotNull(childSlice.get("grandchildren"));

            // All three nested docs share the same root (doc 3): root source loaded exactly once.
            assertEquals(1, storedFieldReads.get());
        });
    }

    private SourceLoader nestedLoader(MapperService mapperService) {
        return mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP, buildNestedDocuments(mapperService));
    }

    private NestedDocuments buildNestedDocuments(MapperService mapperService) {
        return new NestedDocuments(mapperService.mappingLookup(), QueryBitSetProducer::new, IndexVersion.current());
    }

    private StoredFieldLoader storedFieldLoader(SourceLoader loader) {
        return StoredFieldLoader.create(true, loader.requiredStoredFields());
    }

    /**
     * Wraps a LeafReader so that every call to StoredFields.document() increments the counter.
     * This lets tests verify how many times the underlying stored-field store was actually read.
     */
    private LeafReader countingLeafReader(LeafReader delegate, AtomicInteger counter) {
        return new FilterLeafReader(delegate) {
            @Override
            public StoredFields storedFields() throws IOException {
                StoredFields inner = super.storedFields();
                return new StoredFields() {
                    @Override
                    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                        counter.incrementAndGet();
                        inner.document(docID, visitor);
                    }
                };
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };
    }
}
