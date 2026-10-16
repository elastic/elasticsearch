/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

/**
 * The read paths that do not build their terms through a field type: a composite aggregation walking the terms
 * dictionary, a keyword sort reading its first and last term, and the verifier that fails a query which reached it
 * without the slice.
 */
public class SliceReadPathTests extends MapperServiceTestCase {

    @Before
    public void requireSliceIndexing() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    private MapperService sliceMapperService() throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        return createMapperService(settings, mapping(b -> b.startObject("kw").field("type", "keyword").endObject()));
    }

    private static List<String> terms(TermsEnum iterator) throws IOException {
        final List<String> terms = new ArrayList<>();
        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
            terms.add(term.utf8ToString());
        }
        return terms;
    }

    /**
     * A composite aggregation on a keyword source can walk the terms dictionary directly rather than going through the
     * field type. On a slice index those terms carry the slice, so the walk is declined and the aggregation collects
     * from doc values, which hold the plain value. Searching every slice is the case that reaches it: a named slice is
     * already declined by the routing filter leaving the query a boolean.
     */
    public void testCompositeAggregationOverEverySliceReportsPlainTerms() throws IOException {
        final MapperService mapperService = sliceMapperService();
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b")) {
                writer.addDocument(
                    mapperService.documentMapper().parse(source(slice, b -> b.field("kw", "alpha-" + slice), slice)).rootDoc()
                );
            }
            writer.forceMerge(1);
        }, reader -> {
            // The indexed terms carry the slice, so a dictionary walk would hand these back as bucket keys.
            for (String term : terms(reader.leaves().get(0).reader().terms("kw").iterator())) {
                assertThat(term, startsWith(SliceIndexing.termPrefix(term.endsWith("tenant-a") ? "tenant-a" : "tenant-b")));
            }
            // The doc values the aggregation collects from do not.
            final SortedSetDocValues values = reader.leaves().get(0).reader().getSortedSetDocValues("kw");
            final List<String> plain = new ArrayList<>();
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                for (int i = 0; i < values.docValueCount(); i++) {
                    plain.add(values.lookupOrd(values.nextOrd()).utf8ToString());
                }
            }
            assertThat(plain, contains("alpha-tenant-a", "alpha-tenant-b"));
        });
    }

    public void testTheVerifierFailsATermWithoutASlice() throws IOException {
        final MapperService mapperService = sliceMapperService();
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b")) {
                writer.addDocument(mapperService.documentMapper().parse(source(slice, b -> b.field("kw", "alpha"), slice)).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, SliceSelection.of(List.of("tenant-a")));
            final TermsEnum iterator = verified.leaves().get(0).reader().terms("kw").iterator();

            // The prefixed term is what the field type would produce, and passes.
            assertTrue(iterator.seekExact(SliceIndexing.prefixTerm("tenant-a", new BytesRef("alpha"))));

            // A plain term means something built the query without the field type.
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> verified.leaves().get(0).reader().terms("kw").iterator().seekExact(new BytesRef("alpha"))
            );
            assertThat(e.getMessage(), containsString("carries no slice"));

            // So does a term carrying the wrong slice.
            IllegalStateException wrong = expectThrows(
                IllegalStateException.class,
                () -> verified.leaves()
                    .get(0)
                    .reader()
                    .terms("kw")
                    .iterator()
                    .seekExact(SliceIndexing.prefixTerm("tenant-b", new BytesRef("alpha")))
            );
            assertThat(wrong.getMessage(), containsString("carries no slice"));
        });
    }

    public void testTheVerifierFailsAnUnconfinedAutomaton() throws IOException {
        final MapperService mapperService = sliceMapperService();
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("kw", "alpha"), "tenant-a")).rootDoc());
            writer.forceMerge(1);
        }, reader -> {
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, SliceSelection.of(List.of("tenant-a")));
            // A prefix query over the plain value would accept terms with no slice in them at all.
            final var unconfined = new PrefixQuery(new Term("kw", "al")).getAutomaton();
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> verified.leaves().get(0).reader().terms("kw").intersect(new CompiledAutomaton(unconfined, false, false, true), null)
            );
            assertThat(e.getMessage(), containsString("was not restricted to a slice"));
        });
    }

    public void testTheVerifierLeavesReservedFieldsAlone() throws IOException {
        final MapperService mapperService = sliceMapperService();
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("kw", "alpha"), "tenant-a")).rootDoc());
            writer.forceMerge(1);
        }, reader -> {
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, SliceSelection.of(List.of("tenant-a")));
            // _id is looked up without a slice and must not be policed: a plain term must not throw here, whatever it
            // resolves to.
            final TermsEnum ids = verified.leaves().get(0).reader().terms("_id").iterator();
            ids.seekExact(new BytesRef("not-an-id"));
        });
    }

    public void testASearchAcrossSlicesAcceptsAnySlicesTerm() throws IOException {
        final MapperService mapperService = sliceMapperService();
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b")) {
                writer.addDocument(mapperService.documentMapper().parse(source(slice, b -> b.field("kw", "alpha"), slice)).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, SliceSelection.ALL);
            assertTrue(
                verified.leaves()
                    .get(0)
                    .reader()
                    .terms("kw")
                    .iterator()
                    .seekExact(SliceIndexing.prefixTerm("tenant-b", new BytesRef("alpha")))
            );
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> verified.leaves().get(0).reader().terms("kw").iterator().seekExact(new BytesRef("alpha"))
            );
            assertThat(e.getMessage(), containsString("carries no slice"));
        });
    }

    public void testAQueryThroughTheFieldTypePassesTheVerifier() throws IOException {
        final MapperService mapperService = sliceMapperService();
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b")) {
                writer.addDocument(mapperService.documentMapper().parse(source(slice, b -> b.field("kw", "alpha"), slice)).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final var context = createSearchExecutionContext(mapperService);
            context.setSliceSelection(SliceSelection.of(List.of("tenant-a")));
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, SliceSelection.of(List.of("tenant-a")));
            final var searcher = new IndexSearcher(verified);
            searcher.setQueryCache(null);
            // Every shape the field type builds must survive the verifier.
            assertEquals(1, searcher.count(context.getFieldType("kw").termQuery("alpha", context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").prefixQuery("al", null, context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").wildcardQuery("al*", null, context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").regexpQuery("a.pha", 0, 0, 10000, null, context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").termsQuery(List.of("alpha", "beta"), context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").rangeQuery("a", "c", true, true, null, null, null, context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").fuzzyQuery("alpho", Fuzziness.ONE, 0, 50, true, context, null)));
        });
    }

    public void testShapesThatSpanSlicesMatchEverySlice() throws IOException {
        final MapperService mapperService = sliceMapperService();
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b", "tenant-c")) {
                writer.addDocument(mapperService.documentMapper().parse(source(slice, b -> b.field("kw", "alpha"), slice)).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final var context = createSearchExecutionContext(mapperService);
            context.setSliceSelection(SliceSelection.ALL);
            // Through the verifier: a shape that reached the dictionary unconfined fails rather than counting wrongly.
            final var searcher = new IndexSearcher(SliceTermsVerifier.wrap(reader, SliceSelection.ALL));
            searcher.setQueryCache(null);
            assertEquals(3, searcher.count(context.getFieldType("kw").termQuery("alpha", context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").termsQuery(List.of("alpha"), context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").regexpQuery("a.pha", 0, 0, 10000, null, context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").termQueryCaseInsensitive("ALPHA", context)));
            // Every shape spans slices now, none of them error.
            assertEquals(3, searcher.count(context.getFieldType("kw").prefixQuery("al", null, context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").wildcardQuery("al*", null, context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").rangeQuery("a", "c", true, true, null, null, null, context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").fuzzyQuery("alpho", Fuzziness.ONE, 0, 50, true, context, null)));
            assertEquals(0, searcher.count(context.getFieldType("kw").termQuery("missing", context)));
        });
    }

    /**
     * A keyword sort reports the first and last term of the dictionary so the coordinator can order the shards it will
     * ask. On a slice index those terms carry the slice, so there is no bound to report.
     */
    public void testAKeywordSortHasNoShardBoundOnASliceIndex() throws IOException {
        final MapperService sliced = sliceMapperService();
        withLuceneIndex(sliced, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b")) {
                writer.addDocument(sliced.documentMapper().parse(source(slice, b -> b.field("kw", "alpha"), slice)).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final var context = createSearchExecutionContext(sliced, new IndexSearcher(reader));
            assertNull(FieldSortBuilder.getMinMaxOrNull(context, SortBuilders.fieldSort("kw")));
        });

        // The same mapping without slices, where the bound is the value itself.
        final MapperService plain = createMapperService(mapping(b -> b.startObject("kw").field("type", "keyword").endObject()));
        withLuceneIndex(plain, writer -> {
            writer.addDocument(plain.documentMapper().parse(source(b -> b.field("kw", "alpha"))).rootDoc());
            writer.forceMerge(1);
        }, reader -> {
            final var context = createSearchExecutionContext(plain, new IndexSearcher(reader));
            assertEquals(new BytesRef("alpha"), FieldSortBuilder.getMinMaxOrNull(context, SortBuilders.fieldSort("kw")).getMin());
        });
    }
}
