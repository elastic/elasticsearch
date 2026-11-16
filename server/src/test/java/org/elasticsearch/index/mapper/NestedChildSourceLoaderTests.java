/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
                assertEquals(Map.of("items", Map.of("v", "v" + i)), leaf.source(storedFields, i).source());
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
                    assertEquals("doc " + d, expected.source(expectedFields, d).source(), actual.source(actualFields, d).source());
                }
            }
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
