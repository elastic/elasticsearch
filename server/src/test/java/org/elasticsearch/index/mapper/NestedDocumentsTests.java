/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.join.QueryBitSetProducer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

public class NestedDocumentsTests extends MapperServiceTestCase {

    public void testSimpleNestedHierarchy() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("children");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("name").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "top1");
            b.startArray("children");
            {
                b.startObject().field("name", "child1").endObject();
                b.startObject().field("name", "child2").endObject();
                b.startObject().field("name", "child3").endObject();
            }
            b.endArray();
        }));

        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            NestedDocuments nested = new NestedDocuments(mapperService.mappingLookup(), QueryBitSetProducer::new, IndexVersion.current());
            LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

            assertNotNull(leaf.advance(0));
            assertEquals(0, leaf.doc());
            assertEquals(3, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 0, null), leaf.nestedIdentity());

            assertNotNull(leaf.advance(1));
            assertEquals(1, leaf.doc());
            assertEquals(3, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 1, null), leaf.nestedIdentity());

            assertNotNull(leaf.advance(2));
            assertEquals(2, leaf.doc());
            assertEquals(3, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 2, null), leaf.nestedIdentity());

            assertNull(leaf.advance(3));
            assertEquals(3, leaf.doc());
            assertEquals(3, leaf.rootDoc());
            assertNull(leaf.nestedIdentity());
        });

    }

    public void testMultiLevelNestedHierarchy() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("children");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("name").field("type", "keyword").endObject();
                    b.startObject("grandchildren");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        {
                            b.startObject("name").field("type", "keyword").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "top1");
            b.startArray("children");
            {
                b.startObject();
                {
                    b.field("name", "child1");
                    b.startArray("grandchildren");
                    {
                        b.startObject().field("name", "grandchild1").endObject();
                        b.startObject().field("name", "grandchild2").endObject();
                        b.startObject().field("name", "grandchild3").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
                b.startObject();
                {
                    b.field("name", "child2");
                    b.startArray("grandchildren");
                    {
                        b.startObject().field("name", "grandchild21").endObject();
                        b.startObject().field("name", "grandchild22").endObject();
                        b.startObject().field("name", "grandchild23").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
                b.startObject();
                {
                    b.field("name", "child3");
                    b.startArray("grandchildren");
                    {
                        b.startObject().field("name", "grandchild31").endObject();
                        b.startObject().field("name", "grandchild32").endObject();
                        b.startObject().field("name", "grandchild33").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        }));

        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            NestedDocuments nested = new NestedDocuments(mapperService.mappingLookup(), QueryBitSetProducer::new, IndexVersion.current());
            LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

            assertNotNull(leaf.advance(0));
            assertEquals(0, leaf.doc());
            assertEquals(12, leaf.rootDoc());
            assertEquals(
                new SearchHit.NestedIdentity("children", 0, new SearchHit.NestedIdentity("grandchildren", 0, null)),
                leaf.nestedIdentity()
            );

            assertNotNull(leaf.advance(2));
            assertEquals(2, leaf.doc());
            assertEquals(12, leaf.rootDoc());
            assertEquals(
                new SearchHit.NestedIdentity("children", 0, new SearchHit.NestedIdentity("grandchildren", 2, null)),
                leaf.nestedIdentity()
            );

            assertNotNull(leaf.advance(3));
            assertEquals(3, leaf.doc());
            assertEquals(12, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 0, null), leaf.nestedIdentity());

            assertNotNull(leaf.advance(4));
            assertEquals(4, leaf.doc());
            assertEquals(12, leaf.rootDoc());
            assertEquals(
                new SearchHit.NestedIdentity("children", 1, new SearchHit.NestedIdentity("grandchildren", 0, null)),
                leaf.nestedIdentity()
            );

            assertNotNull(leaf.advance(5));
            assertEquals(5, leaf.doc());
            assertEquals(12, leaf.rootDoc());
            assertEquals(
                new SearchHit.NestedIdentity("children", 1, new SearchHit.NestedIdentity("grandchildren", 1, null)),
                leaf.nestedIdentity()
            );

            assertNull(leaf.advance(12));
            assertNull(leaf.nestedIdentity());
        });
    }

    /**
     * Regression test for a null-pointer exception in {@code findObjectPath} when callers access documents
     * in non-ascending order.  The rewind path re-obtains a fresh scorer via {@code Weight.scorer(ctx)},
     * which may return {@code null} if the segment contains no matching nested docs for that path.
     * Before the fix, the null was stored into the map and immediately dereferenced.
     *
     * This test exercises the rewind code path by advancing a {@link LeafNestedDocuments} to a later
     * nested doc, then back to an earlier one within the same segment.
     */
    public void testFindObjectPathBackwardDocAccess() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("children");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("name").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        // Two root documents, each with nested children, in a single segment.
        ParsedDocument doc1 = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "root1");
            b.startArray("children");
            {
                b.startObject().field("name", "child1").endObject();
                b.startObject().field("name", "child2").endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc2 = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "root2");
            b.startArray("children");
            {
                b.startObject().field("name", "child3").endObject();
                b.startObject().field("name", "child4").endObject();
            }
            b.endArray();
        }));

        withLuceneIndex(mapperService, iw -> {
            iw.addDocuments(doc1.docs());
            iw.addDocuments(doc2.docs());
            iw.forceMerge(1);
        }, reader -> {
            // Segment layout (single segment after forceMerge):
            // doc 0: nested child1 (root1)
            // doc 1: nested child2 (root1)
            // doc 2: root1 (parent)
            // doc 3: nested child3 (root2)
            // doc 4: nested child4 (root2)
            // doc 5: root2 (parent)
            NestedDocuments nested = new NestedDocuments(mapperService.mappingLookup(), QueryBitSetProducer::new, IndexVersion.current());
            LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

            // Advance forward to doc 3 (child3 under root2) — this moves the child scorer iterator forward.
            assertNotNull(leaf.advance(3));
            assertEquals(3, leaf.doc());
            assertEquals(5, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 0, null), leaf.nestedIdentity());

            // Now advance backward to doc 0 (child1 under root1).
            // This triggers the rewind path in findObjectPath because the scorer iterator is past doc 0.
            assertNotNull(leaf.advance(0));
            assertEquals(0, leaf.doc());
            assertEquals(2, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 0, null), leaf.nestedIdentity());

            // Verify forward access still works after a rewind.
            assertNotNull(leaf.advance(4));
            assertEquals(4, leaf.doc());
            assertEquals(5, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 1, null), leaf.nestedIdentity());
        });
    }

    public void testNestedObjectWithinNonNestedObject() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("children");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("name").field("type", "keyword").endObject();
                    b.startObject("grandchildren");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        {
                            b.startObject("name").field("type", "keyword").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "top1");
            b.startArray("children");
            {
                b.startObject();
                {
                    b.field("name", "child1");
                    b.startArray("grandchildren");
                    {
                        b.startObject().field("name", "grandchild1").endObject();
                        b.startObject().field("name", "grandchild2").endObject();
                        b.startObject().field("name", "grandchild3").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
                b.startObject();
                {
                    b.field("name", "child2");
                    b.startArray("grandchildren");
                    {
                        b.startObject().field("name", "grandchild21").endObject();
                        b.startObject().field("name", "grandchild22").endObject();
                        b.startObject().field("name", "grandchild23").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
                b.startObject();
                {
                    b.field("name", "child3");
                    b.startArray("grandchildren");
                    {
                        b.startObject().field("name", "grandchild31").endObject();
                        b.startObject().field("name", "grandchild32").endObject();
                        b.startObject().field("name", "grandchild33").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        }));

        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            NestedDocuments nested = new NestedDocuments(mapperService.mappingLookup(), QueryBitSetProducer::new, IndexVersion.current());
            LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

            assertNotNull(leaf.advance(0));
            assertEquals(0, leaf.doc());
            assertEquals(9, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children.grandchildren", 0, null), leaf.nestedIdentity());

            assertNotNull(leaf.advance(2));
            assertEquals(2, leaf.doc());
            assertEquals(9, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children.grandchildren", 2, null), leaf.nestedIdentity());

            assertNotNull(leaf.advance(3));
            assertEquals(3, leaf.doc());
            assertEquals(9, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children.grandchildren", 3, null), leaf.nestedIdentity());

            assertNotNull(leaf.advance(5));
            assertEquals(5, leaf.doc());
            assertEquals(9, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children.grandchildren", 5, null), leaf.nestedIdentity());

            assertNull(leaf.advance(9));
            assertNull(leaf.nestedIdentity());
        });
    }

}
