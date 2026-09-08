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
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

public class NestedDocumentsTests extends MapperServiceTestCase {

    /**
     * A {@code TermQuery} weight built for no-scores resolves term existence lazily via {@code TermStates}. When the terms dictionary
     * cannot cheaply prove the term is absent, the first {@code scorer(ctx)} call runs the deferred seek, finds the term absent,
     * memoizes {@code EMPTY_TERMSTATE}, and returns a non-null scorer over an empty iterator. Because that first scorer is non-null, the
     * constructor stores the absent path in {@code childScorers}. Any subsequent {@code scorer(ctx)} call then returns null (the memoized
     * absence surfaces at the nullable supplier stage). The rewind path re-pulls a fresh scorer for backward doc access, so it hits exactly
     * that second-call null and must skip the path instead of dereferencing it.
     */
    public void testFindObjectPathAbsentNestedPathRewind() throws IOException {
        int present = 40;
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            // Many present nested fields n00..n39 populate _nested_path with enough terms to build real block-tree blocks.
            for (int i = 0; i < present; i++) {
                b.startObject(Strings.format("n%02d", i));
                b.field("type", "nested");
                b.startObject("properties").startObject("name").field("type", "keyword").endObject().endObject();
                b.endObject();
            }

            // Absent interior nested fields: mapped (so they are scored) but never populated, so their _nested_path term is absent.
            // At least one of these sorts inside a present block and takes the non-cheap absent-term path (first scorer non-null empty).
            for (String absent : new String[] { "n05z", "n10z", "n20z", "n30z" }) {
                b.startObject(absent);
                b.field("type", "nested");
                b.startObject("properties").startObject("name").field("type", "keyword").endObject().endObject();
                b.endObject();
            }
        }));

        // One root with a single child under each present nested field: children get doc ids 0..present-1, the root is the last doc.
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "top1");
            for (int i = 0; i < present; i++) {
                b.startArray(Strings.format("n%02d", i));
                b.startObject().field("name", randomAlphanumericOfLength(6)).endObject();
                b.endArray();
            }
        }));

        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            // Guard the coverage itself: at least one absent interior path must return a non-null empty scorer on the first scorer(ctx)
            // call and null on re-pull. If none do, childScorers holds no empty scorers, the rewind never returns null, and this test would
            // pass while exercising nothing. Fail loudly in that case instead.
            assertTrue(
                "no absent interior nested path returned a non-null empty scorer then null; the rewind null-scorer branch is not exercised",
                anyAbsentPathReturnsEmptyThenNullScorer(mapperService, reader.leaves().get(0), "n05z", "n10z", "n20z", "n30z")
            );

            NestedDocuments nested = new NestedDocuments(mapperService.mappingLookup(), QueryBitSetProducer::new, IndexVersion.current());
            LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

            // Forward to the last child, parking every child scorer (including the absent empty ones) at or past that doc.
            assertNotNull(leaf.advance(present - 1));
            assertEquals(present, leaf.rootDoc());

            // Backward to the first child. This rewinds every parked scorer; the absent empty scorers re-pull as null and must be
            // skipped by the guard rather than dereferenced. Before the guard this threw a NullPointerException.
            assertNotNull(leaf.advance(0));
            assertEquals(present, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("n00", 0, null), leaf.nestedIdentity());
        });
    }

    /**
     * True if any of the given mapped-but-unpopulated nested paths returns a non-null empty scorer on the first {@code Weight.scorer(ctx)}
     * call and null on the second.
     */
    private static boolean anyAbsentPathReturnsEmptyThenNullScorer(MapperService mapperService, LeafReaderContext ctx, String... paths)
        throws IOException {
        for (String path : paths) {
            NestedObjectMapper mapper = mapperService.mappingLookup().nestedLookup().getNestedMappers().get(path);
            IndexSearcher searcher = new IndexSearcher(ReaderUtil.getTopLevelContext(ctx));
            Weight weight = searcher.createWeight(searcher.rewrite(mapper.nestedTypeFilter()), ScoreMode.COMPLETE_NO_SCORES, 1);

            // First scorer(ctx) runs the deferred term lookup; a non-cheap absent term returns a non-null empty scorer here and null after.
            if (weight.scorer(ctx) != null && weight.scorer(ctx) == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Coverage for the constructor's null-scorer gate (the {@code scorer != null} check). An absent nested path whose term the terms
     * dictionary can prove missing cheaply returns null on the very first {@code scorer(ctx)} call, so the path is never stored in
     * {@code childScorers} and never reaches {@code findObjectPath}. This is the companion to
     * {@link #testFindObjectPathAbsentNestedPathRewind}, where the absent term is not cheaply provable and its first scorer is a non-null
     * empty scorer instead of null. Here the sole
     * populated nested-path term is "children"; the mapped-but-unpopulated "unpopulated" sorts past it, so its absence is proven from the
     * max-term boundary with no I/O. Without the gate the null scorer would be stored and dereferenced in findObjectPath.
     */
    public void testConstructorSkipsCheapNullAbsentNestedPath() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("children");
            {
                b.field("type", "nested");
                b.startObject("properties").startObject("name").field("type", "keyword").endObject().endObject();
            }
            b.endObject();
            // Mapped but never populated: its _nested_path term is absent and (sorting past "children") cheaply provable absent.
            b.startObject("unpopulated");
            {
                b.field("type", "nested");
                b.startObject("properties").startObject("name").field("type", "keyword").endObject().endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "top1");
            b.startArray("children");
            {
                b.startObject().field("name", randomAlphanumericOfLength(6)).endObject();
                b.startObject().field("name", randomAlphanumericOfLength(6)).endObject();
            }
            b.endArray();
        }));

        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            NestedDocuments nested = new NestedDocuments(mapperService.mappingLookup(), QueryBitSetProducer::new, IndexVersion.current());
            LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

            // Forward reads resolve normally; the cheap-null absent path was filtered at construction, so it never interferes.
            assertNotNull(leaf.advance(0));
            assertEquals(new SearchHit.NestedIdentity("children", 0, null), leaf.nestedIdentity());
            assertNotNull(leaf.advance(1));
            assertEquals(new SearchHit.NestedIdentity("children", 1, null), leaf.nestedIdentity());

            // Backward read still works: the only scorer in play is "children"; the absent path was never stored.
            assertNotNull(leaf.advance(0));
            assertEquals(new SearchHit.NestedIdentity("children", 0, null), leaf.nestedIdentity());
        });
    }

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

    /**
     * Asingle {@link LeafNestedDocuments} may be advanced backwards when a stored-source loader re-reads the same segment for a later query
     * clause. The shared child scorer in findObjectPath is a forward-only iterator, so a lower doc id after a higher one used to throw
     * "Cannot find object path for document". Here one child scorer exists (single nested level); advancing to child doc 2 then back to
     * docs 1 and 0 must resolve each child instead of throwing.
     */
    public void testSimpleNestedHierarchyBackwardDocAccess() throws IOException {

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

            // Advance forward to the last child first, parking the shared child scorer at doc 2.
            assertNotNull(leaf.advance(2));
            assertEquals(2, leaf.doc());
            assertEquals(3, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 2, null), leaf.nestedIdentity());

            // Now walk backwards: each advance must rewind the parked scorer rather than throw.
            assertNotNull(leaf.advance(1));
            assertEquals(1, leaf.doc());
            assertEquals(3, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 1, null), leaf.nestedIdentity());

            assertNotNull(leaf.advance(0));
            assertEquals(0, leaf.doc());
            assertEquals(3, leaf.rootDoc());
            assertEquals(new SearchHit.NestedIdentity("children", 0, null), leaf.nestedIdentity());
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
