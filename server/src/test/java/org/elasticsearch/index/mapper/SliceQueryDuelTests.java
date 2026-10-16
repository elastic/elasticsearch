/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Proves that a slice index answers a query exactly as a plain index holding only that slice would.
 *
 * <p>The same documents are indexed twice: once into a slice index, where the terms carry the slice, and once per slice
 * into a plain index holding only that slice's documents. Every query is then built by the field type against both and
 * the matching documents compared. Nothing in the test knows about the prefix, which is the point: if a query shape
 * forgets it, the slice index returns nothing and the duel fails.
 */
public class SliceQueryDuelTests extends MapperServiceTestCase {

    @Before
    public void requireSliceIndexing() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    private static final List<String> SLICES = List.of("tenant-a", "tenant-b", "tenant-c");

    /** name -> slice, keyword, text. Values deliberately overlap across slices. */
    private static final Map<String, String[]> CORPUS = new LinkedHashMap<>();
    static {
        CORPUS.put("a1", new String[] { "tenant-a", "alpha", "the quick brown fox" });
        CORPUS.put("a2", new String[] { "tenant-a", "beta", "a lazy brown dog" });
        CORPUS.put("a3", new String[] { "tenant-a", "alphabet", "quick quick fox" });
        CORPUS.put("b1", new String[] { "tenant-b", "alpha", "the quick red fox" });
        CORPUS.put("b2", new String[] { "tenant-b", "gamma", "a sleepy brown cat" });
        CORPUS.put("b3", new String[] { "tenant-b", "beta", "lazy lazy dog" });
        CORPUS.put("c1", new String[] { "tenant-c", "beta", "the quick brown fox" });
        CORPUS.put("c2", new String[] { "tenant-c", "delta", "a brown bear" });
    }

    /**
     * {@code uppercase} is deliberately not idempotent over the slice prefix, which is lowercase hex: a query that put the
     * slice on before normalizing would ask for an upper-cased prefix and match nothing.
     */
    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.of("uppercase", new NamedAnalyzer("uppercase", AnalyzerScope.INDEX, new UppercaseNormalizer()))
        );
    }

    private static final class UppercaseNormalizer extends Analyzer {
        @Override
        protected TokenStreamComponents createComponents(String field) {
            final Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, new UpperCaseFilter(tokenizer));
        }

        @Override
        protected TokenStream normalize(String field, TokenStream in) {
            return new UpperCaseFilter(in);
        }
    }

    private MapperService mapperService(boolean sliced) throws IOException {
        final Settings settings = sliced ? Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build() : Settings.EMPTY;
        return createMapperService(settings, mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("kw").field("type", "keyword").endObject();
            b.startObject("norm").field("type", "keyword").field("normalizer", "uppercase").endObject();
            b.startObject("uni").field("type", "keyword").endObject();
            b.startObject("body").field("type", "text").endObject();
            b.startObject("sv").field("type", "sparse_vector").endObject();
        }));
    }

    /** Runs {@code query} against a slice index scoped to {@code slice}, and against a plain index of that slice only. */
    private void duel(String description, CheckedFunction<SearchExecutionContext, Query, IOException> query) throws IOException {
        for (String slice : SLICES) {
            final List<String> sliced = searchSliced(slice, query);
            final List<String> plain = searchPlain(slice, query);
            assertThat(description + " on [" + slice + "]", sliced, equalTo(plain));
        }
    }

    private List<String> searchSliced(String slice, CheckedFunction<SearchExecutionContext, Query, IOException> query) throws IOException {
        final MapperService mapperService = mapperService(true);
        final List<String> names = new ArrayList<>();
        withLuceneIndex(mapperService, writer -> {
            for (var entry : CORPUS.entrySet()) {
                writer.addDocument(parse(mapperService, entry).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            context.setSliceSelection(SliceSelection.fromSearchSlice(slice));
            names.addAll(matches(reader, query.apply(context)));
        });
        return names;
    }

    /** The same query over several slices at once, against a plain index holding exactly those slices. */
    private void duelAcross(String description, List<String> slices, CheckedFunction<SearchExecutionContext, Query, IOException> query)
        throws IOException {
        final List<String> sliced = searchSliced(String.join(",", slices), query);
        final List<String> plain = searchPlain(slices, query);
        assertThat(description + " across " + slices, sliced, equalTo(plain));
    }

    /** The same query with every slice in scope, against a plain index holding all of them. */
    private void duelAcrossEverySlice(String description, CheckedFunction<SearchExecutionContext, Query, IOException> query)
        throws IOException {
        final List<String> sliced = searchSliced(SliceIndexing.SLICE_ALL, query);
        final List<String> plain = searchPlain(SLICES, query);
        assertThat(description + " across every slice", sliced, equalTo(plain));
    }

    private List<String> searchPlain(String slice, CheckedFunction<SearchExecutionContext, Query, IOException> query) throws IOException {
        return searchPlain(List.of(slice), query);
    }

    private List<String> searchPlain(List<String> slices, CheckedFunction<SearchExecutionContext, Query, IOException> query)
        throws IOException {
        final MapperService mapperService = mapperService(false);
        final List<String> names = new ArrayList<>();
        withLuceneIndex(mapperService, writer -> {
            for (var entry : CORPUS.entrySet()) {
                if (slices.contains(entry.getValue()[0])) {
                    writer.addDocument(parse(mapperService, entry).rootDoc());
                }
            }
            writer.forceMerge(1);
        }, reader -> names.addAll(matches(reader, query.apply(createSearchExecutionContext(mapperService)))));
        return names;
    }

    private ParsedDocument parse(MapperService mapperService, Map.Entry<String, String[]> entry) throws IOException {
        final String name = entry.getKey();
        final String[] fields = entry.getValue();
        return mapperService.documentMapper().parse(source(name, b -> {
            b.field("name", name);
            b.field("kw", fields[1]);
            b.field("norm", fields[1]);
            // A value and a token outside ASCII: a term is bytes, so a shape that reasons in characters answers wrongly here.
            b.field("uni", "café-" + fields[1]);
            b.field("body", fields[2] + " café");
            // One feature every document shares and one that follows the keyword, so a slice-scoped query has to tell them apart.
            b.startObject("sv").field("shared", 1.0).field(fields[1], 2.0).endObject();
        }, mapperService.getIndexSettings().isSliceEnabled() ? fields[0] : null));
    }

    /** The names of the matching documents, sorted, read from the plain doc values. */
    private static List<String> matches(DirectoryReader reader, Query query) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        final ScoreDoc[] hits = searcher.search(query, 100).scoreDocs;
        final List<String> names = new ArrayList<>();
        for (ScoreDoc hit : hits) {
            final LeafReaderContext leaf = reader.leaves().get(ReaderUtil.subIndex(hit.doc, reader.leaves()));
            final SortedSetDocValues values = leaf.reader().getSortedSetDocValues("name");
            if (values != null && values.advanceExact(hit.doc - leaf.docBase)) {
                names.add(values.lookupOrd(values.nextOrd()).utf8ToString());
            }
        }
        names.sort(String::compareTo);
        return names;
    }

    public void testTermQuery() throws IOException {
        for (String value : List.of("alpha", "beta", "gamma", "delta", "missing")) {
            duel("term " + value, context -> context.getFieldType("kw").termQuery(value, context));
        }
        for (String value : List.of("brown", "quick", "lazy", "missing")) {
            duel("text term " + value, context -> context.getFieldType("body").termQuery(value, context));
        }
    }

    /**
     * {@code sparse_vector} stores a feature as a term and its weight as the term frequency, and {@link
     * org.apache.lucene.document.FeatureField} can only name one term, so the shape asks each targeted slice for its own copy.
     */
    public void testSparseVectorFeatureQuery() throws IOException {
        for (String feature : List.of("shared", "alpha", "beta", "gamma", "delta", "missing")) {
            duel("sparse_vector feature " + feature, context -> context.getFieldType("sv").termQuery(feature, context));
        }
    }

    public void testSparseVectorFeatureQueryAcrossSlices() throws IOException {
        duelAcross(
            "sparse_vector feature",
            List.of("tenant-a", "tenant-b"),
            context -> context.getFieldType("sv").termQuery("shared", context)
        );
    }

    /** A feature is scored one term at a time, so there is no form that reaches every slice at once. */
    public void testSparseVectorFeatureQueryOverEverySliceIsDeclined() throws IOException {
        final MapperService mapperService = mapperService(true);
        withLuceneIndex(mapperService, writer -> {
            for (var entry : CORPUS.entrySet()) {
                writer.addDocument(parse(mapperService, entry).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            context.setSliceSelection(SliceSelection.ALL);
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> context.getFieldType("sv").termQuery("shared", context)
            );
            assertThat(e.getMessage(), containsString("scores one term at a time"));
        });
    }

    public void testTermsQuery() throws IOException {
        duel("terms", context -> context.getFieldType("kw").termsQuery(List.of("alpha", "gamma", "missing"), context));
    }

    public void testTermQueryCaseInsensitive() throws IOException {
        duel("term ci", context -> context.getFieldType("kw").termQueryCaseInsensitive("ALPHA", context));
    }

    public void testPrefixQuery() throws IOException {
        for (String value : List.of("al", "alpha", "b", "zzz")) {
            duel("prefix " + value, context -> context.getFieldType("kw").prefixQuery(value, null, context));
        }
    }

    public void testWildcardQuery() throws IOException {
        for (String value : List.of("al*", "*a", "*et*", "?eta", "zzz*")) {
            duel("wildcard " + value, context -> context.getFieldType("kw").wildcardQuery(value, null, context));
        }
    }

    public void testWildcardQueryCaseInsensitive() throws IOException {
        for (String value : List.of("AL*", "*A", "?ETA")) {
            duel(
                "wildcard ci " + value,
                context -> ((StringFieldType) context.getFieldType("kw")).wildcardQuery(value, null, true, context)
            );
        }
    }

    public void testRegexpQuery() throws IOException {
        for (String value : List.of("a.pha", "alpha.*", "(alpha|gamma)", "zzz")) {
            duel("regexp " + value, context -> context.getFieldType("kw").regexpQuery(value, 0, 0, 10000, null, context));
        }
    }

    public void testFuzzyQuery() throws IOException {
        for (String value : List.of("alpho", "betta", "gamma", "zzzzz")) {
            duel("fuzzy " + value, context -> context.getFieldType("kw").fuzzyQuery(value, Fuzziness.ONE, 0, 50, true, context, null));
        }
    }

    public void testRangeQuery() throws IOException {
        duel("range a..c", context -> context.getFieldType("kw").rangeQuery("a", "c", true, true, null, null, null, context));
        duel("range null..c", context -> context.getFieldType("kw").rangeQuery(null, "c", true, true, null, null, null, context));
        duel("range a..null", context -> context.getFieldType("kw").rangeQuery("a", null, true, true, null, null, null, context));
        duel("range null..null", context -> context.getFieldType("kw").rangeQuery(null, null, true, true, null, null, null, context));
        duel(
            "range exclusive",
            context -> context.getFieldType("kw").rangeQuery("alpha", "delta", false, false, null, null, null, context)
        );
    }

    private static TextFieldMapper.TextFieldType body(SearchExecutionContext context) {
        return (TextFieldMapper.TextFieldType) context.getFieldType("body");
    }

    private static Query intervalQuery(IntervalsSource source) {
        return new IntervalQuery("body", source);
    }

    public void testSeveralSlicesAtOnce() throws IOException {
        final List<String> two = List.of("tenant-a", "tenant-b");
        duelAcross("term", two, context -> context.getFieldType("kw").termQuery("alpha", context));
        duelAcross("terms", two, context -> context.getFieldType("kw").termsQuery(List.of("alpha", "beta"), context));
        duelAcross("prefix", two, context -> context.getFieldType("kw").prefixQuery("al", null, context));
        duelAcross("wildcard", two, context -> context.getFieldType("kw").wildcardQuery("*et*", null, context));
        duelAcross("regexp", two, context -> context.getFieldType("kw").regexpQuery("a.pha", 0, 0, 10000, null, context));
        duelAcross("range", two, context -> context.getFieldType("kw").rangeQuery("a", "c", true, true, null, null, null, context));
        duelAcross("range open", two, context -> context.getFieldType("kw").rangeQuery("a", null, true, true, null, null, null, context));
        duelAcross("fuzzy", two, context -> context.getFieldType("kw").fuzzyQuery("alpho", Fuzziness.ONE, 0, 50, true, context, null));
        duelAcross("term ci", two, context -> context.getFieldType("kw").termQueryCaseInsensitive("ALPHA", context));
        duelAcross("text term", two, context -> context.getFieldType("body").termQuery("brown", context));
    }

    public void testSeveralSlicesAtOnceForIntervals() throws IOException {
        final List<String> two = List.of("tenant-a", "tenant-c");
        duelAcross("term intervals", two, context -> intervalQuery(body(context).termIntervals(new BytesRef("brown"), context)));
        duelAcross("prefix intervals", two, context -> intervalQuery(body(context).prefixIntervals(new BytesRef("br"), context)));
        duelAcross("wildcard intervals", two, context -> intervalQuery(body(context).wildcardIntervals(new BytesRef("bro*"), context)));
        duelAcross("fuzzy intervals", two, context -> intervalQuery(body(context).fuzzyIntervals("brwon", 1, 0, true, context)));
        duelAcross(
            "range intervals",
            two,
            context -> intervalQuery(body(context).rangeIntervals(new BytesRef("b"), new BytesRef("d"), true, true, context))
        );
        duelAcross(
            "phrase intervals",
            two,
            context -> intervalQuery(
                Intervals.ordered(
                    body(context).termIntervals(new BytesRef("quick"), context),
                    body(context).termIntervals(new BytesRef("brown"), context)
                )
            )
        );
    }

    public void testTermIntervals() throws IOException {
        for (String term : List.of("brown", "quick", "lazy", "missing")) {
            duel("term intervals " + term, context -> intervalQuery(body(context).termIntervals(new BytesRef(term), context)));
        }
    }

    public void testPrefixIntervals() throws IOException {
        for (String term : List.of("br", "qu", "zzz")) {
            duel("prefix intervals " + term, context -> intervalQuery(body(context).prefixIntervals(new BytesRef(term), context)));
        }
    }

    public void testWildcardIntervals() throws IOException {
        for (String pattern : List.of("bro*", "*own", "?uick", "zzz*")) {
            duel(
                "wildcard intervals " + pattern,
                context -> intervalQuery(body(context).wildcardIntervals(new BytesRef(pattern), context))
            );
        }
    }

    public void testRegexpIntervals() throws IOException {
        for (String pattern : List.of("b.own", "qu.*", "(brown|quick)", "zzz")) {
            duel("regexp intervals " + pattern, context -> intervalQuery(body(context).regexpIntervals(new BytesRef(pattern), context)));
        }
    }

    public void testFuzzyIntervals() throws IOException {
        for (String term : List.of("brwon", "quik", "brown", "zzzzz")) {
            duel("fuzzy intervals " + term, context -> intervalQuery(body(context).fuzzyIntervals(term, 1, 0, true, context)));
        }
    }

    public void testRangeIntervals() throws IOException {
        duel(
            "range intervals b..d",
            context -> intervalQuery(body(context).rangeIntervals(new BytesRef("b"), new BytesRef("d"), true, true, context))
        );
        duel(
            "range intervals open upper",
            context -> intervalQuery(body(context).rangeIntervals(new BytesRef("b"), null, true, true, context))
        );
        duel(
            "range intervals open lower",
            context -> intervalQuery(body(context).rangeIntervals(null, new BytesRef("d"), true, true, context))
        );
    }

    /** A phrase has to line up positions, so it fails if the per-slice sources are stitched together wrongly. */
    public void testPhraseIntervals() throws IOException {
        duel(
            "phrase intervals",
            context -> intervalQuery(
                Intervals.ordered(
                    body(context).termIntervals(new BytesRef("quick"), context),
                    body(context).termIntervals(new BytesRef("brown"), context)
                )
            )
        );
        duel(
            "phrase intervals reversed",
            context -> intervalQuery(
                Intervals.ordered(
                    body(context).termIntervals(new BytesRef("brown"), context),
                    body(context).termIntervals(new BytesRef("quick"), context)
                )
            )
        );
    }

    /**
     * A shape that cannot name a slice is confined to any slice's prefix instead, so searching every slice must find what a
     * plain index holding every document finds.
     */
    public void testEverySliceAtOnce() throws IOException {
        duelAcrossEverySlice("term", context -> context.getFieldType("kw").termQuery("alpha", context));
        duelAcrossEverySlice("term missing", context -> context.getFieldType("kw").termQuery("missing", context));
        duelAcrossEverySlice("terms", context -> context.getFieldType("kw").termsQuery(List.of("alpha", "gamma"), context));
        duelAcrossEverySlice("term ci", context -> context.getFieldType("kw").termQueryCaseInsensitive("ALPHA", context));
        duelAcrossEverySlice("prefix", context -> context.getFieldType("kw").prefixQuery("al", null, context));
        duelAcrossEverySlice("wildcard", context -> context.getFieldType("kw").wildcardQuery("*et*", null, context));
        duelAcrossEverySlice(
            "wildcard ci",
            context -> ((StringFieldType) context.getFieldType("kw")).wildcardQuery("*ET*", null, true, context)
        );
        duelAcrossEverySlice("regexp", context -> context.getFieldType("kw").regexpQuery("a.pha", 0, 0, 10000, null, context));
        duelAcrossEverySlice("range", context -> context.getFieldType("kw").rangeQuery("a", "c", true, true, null, null, null, context));
        duelAcrossEverySlice(
            "range open",
            context -> context.getFieldType("kw").rangeQuery("a", null, true, true, null, null, null, context)
        );
        duelAcrossEverySlice("fuzzy", context -> context.getFieldType("kw").fuzzyQuery("alpho", Fuzziness.ONE, 0, 50, true, context, null));
        duelAcrossEverySlice("text term", context -> context.getFieldType("body").termQuery("brown", context));
    }

    public void testEverySliceAtOnceForIntervals() throws IOException {
        duelAcrossEverySlice("term intervals", context -> intervalQuery(body(context).termIntervals(new BytesRef("brown"), context)));
        duelAcrossEverySlice("prefix intervals", context -> intervalQuery(body(context).prefixIntervals(new BytesRef("br"), context)));
        duelAcrossEverySlice(
            "wildcard intervals",
            context -> intervalQuery(body(context).wildcardIntervals(new BytesRef("bro*"), context))
        );
        duelAcrossEverySlice("regexp intervals", context -> intervalQuery(body(context).regexpIntervals(new BytesRef("b.own"), context)));
        duelAcrossEverySlice("fuzzy intervals", context -> intervalQuery(body(context).fuzzyIntervals("brwon", 1, 0, true, context)));
        duelAcrossEverySlice(
            "range intervals",
            context -> intervalQuery(body(context).rangeIntervals(new BytesRef("b"), new BytesRef("d"), true, true, context))
        );
        duelAcrossEverySlice(
            "phrase intervals",
            context -> intervalQuery(
                Intervals.ordered(
                    body(context).termIntervals(new BytesRef("quick"), context),
                    body(context).termIntervals(new BytesRef("brown"), context)
                )
            )
        );
    }

    /**
     * A term is a string of bytes, so a shape built around characters has to be converted before the slice goes in front
     * of it. Every multi-byte character is a character where that conversion is the difference between a match and none.
     */
    public void testAValueOutsideAsciiIsAskedForAsBytes() throws IOException {
        final List<String> two = List.of("tenant-a", "tenant-b");
        for (var shape : List.<Map.Entry<String, CheckedFunction<SearchExecutionContext, Query, IOException>>>of(
            Map.entry("term", context -> context.getFieldType("uni").termQuery("café-alpha", context)),
            Map.entry("terms", context -> context.getFieldType("uni").termsQuery(List.of("café-alpha", "café-beta"), context)),
            Map.entry("term ci", context -> context.getFieldType("uni").termQueryCaseInsensitive("CAFÉ-ALPHA", context)),
            Map.entry("prefix", context -> context.getFieldType("uni").prefixQuery("café", null, context)),
            Map.entry("prefix ci", context -> ((StringFieldType) context.getFieldType("uni")).prefixQuery("CAFÉ", null, true, context)),
            // a single-character wildcard has to match every byte of the character it stands for
            Map.entry("wildcard", context -> context.getFieldType("uni").wildcardQuery("caf?-alpha", null, context)),
            Map.entry("wildcard tail", context -> context.getFieldType("uni").wildcardQuery("*é-alpha", null, context)),
            Map.entry(
                "wildcard ci",
                context -> ((StringFieldType) context.getFieldType("uni")).wildcardQuery("CAF?-ALPHA", null, true, context)
            ),
            Map.entry("regexp", context -> context.getFieldType("uni").regexpQuery("caf.-alpha", 0, 0, 10000, null, context)),
            Map.entry("range", context -> context.getFieldType("uni").rangeQuery("café", "cafz", true, true, null, null, null, context)),
            Map.entry("fuzzy", context -> context.getFieldType("uni").fuzzyQuery("café-alpho", Fuzziness.ONE, 0, 50, true, context, null)),
            Map.entry("term intervals", context -> intervalQuery(body(context).termIntervals(new BytesRef("café"), context))),
            Map.entry("prefix intervals", context -> intervalQuery(body(context).prefixIntervals(new BytesRef("caf"), context))),
            Map.entry("wildcard intervals", context -> intervalQuery(body(context).wildcardIntervals(new BytesRef("caf?"), context))),
            Map.entry("regexp intervals", context -> intervalQuery(body(context).regexpIntervals(new BytesRef("caf."), context))),
            Map.entry("fuzzy intervals", context -> intervalQuery(body(context).fuzzyIntervals("cafè", 1, 0, true, context)))
        )) {
            duel(shape.getKey(), shape.getValue());
            duelAcross(shape.getKey(), two, shape.getValue());
            duelAcrossEverySlice(shape.getKey(), shape.getValue());
        }
    }

    /** The duel above would pass if the non-ASCII term matched nothing anywhere, so this pins one of the answers. */
    public void testTheValueOutsideAsciiMatchesSomething() throws IOException {
        assertThat(
            searchSliced("tenant-a", context -> context.getFieldType("uni").wildcardQuery("caf?-alpha", null, context)),
            equalTo(List.of("a1"))
        );
    }

    /**
     * A normalized field decides what its terms look like, so the slice has to go on after the normalizer has run. The
     * normalizer here upper-cases, which the lowercase hex prefix would not survive.
     */
    public void testANormalizedFieldIsAskedForInItsIndexedForm() throws IOException {
        duel("normalized term", context -> context.getFieldType("norm").termQuery("alpha", context));
        duel("normalized terms", context -> context.getFieldType("norm").termsQuery(List.of("alpha", "gamma"), context));
        duel("normalized prefix", context -> context.getFieldType("norm").prefixQuery("al", null, context));
        duel("normalized wildcard", context -> context.getFieldType("norm").wildcardQuery("al*", null, context));
        duel("normalized wildcard mid", context -> context.getFieldType("norm").wildcardQuery("*et*", null, context));
        duel("normalized wildcard normalizing", context -> context.getFieldType("norm").normalizedWildcardQuery("al*", null, context));
        duel("normalized regexp", context -> context.getFieldType("norm").regexpQuery("A.PHA", 0, 0, 10000, null, context));
        duel("normalized range", context -> context.getFieldType("norm").rangeQuery("A", "C", true, true, null, null, null, context));
        duel("normalized fuzzy", context -> context.getFieldType("norm").fuzzyQuery("alpho", Fuzziness.ONE, 0, 50, true, context, null));
        duelAcross("normalized term", List.of("tenant-a", "tenant-b"), c -> c.getFieldType("norm").termQuery("alpha", c));
        duelAcrossEverySlice("normalized term", context -> context.getFieldType("norm").termQuery("alpha", context));
        duelAcrossEverySlice("normalized wildcard", context -> context.getFieldType("norm").wildcardQuery("al*", null, context));
    }

    /** The corpus has to distinguish the slices for the normalized field too, not only report nothing everywhere. */
    public void testTheNormalizedFieldMatchesSomething() throws IOException {
        assertThat(searchSliced("tenant-a", context -> context.getFieldType("norm").termQuery("alpha", context)), equalTo(List.of("a1")));
    }

    /**
     * The duel would also pass if nothing matched anywhere, so this checks the corpus actually distinguishes the slices:
     * a term every slice holds must match different documents in each.
     */
    public void testTheCorpusWouldCatchALeak() throws IOException {
        final List<String> a = searchSliced("tenant-a", context -> context.getFieldType("kw").termQuery("alpha", context));
        final List<String> b = searchSliced("tenant-b", context -> context.getFieldType("kw").termQuery("alpha", context));
        assertThat(a, equalTo(List.of("a1")));
        assertThat(b, equalTo(List.of("b1")));
        assertThat(a, not(equalTo(b)));
    }
}
