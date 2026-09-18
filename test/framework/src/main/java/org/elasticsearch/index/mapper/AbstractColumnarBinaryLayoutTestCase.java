/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.query.IntervalQueryBuilder;
import org.elasticsearch.index.query.IntervalsSourceProvider;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Reads a field back in each of the layouts its binary doc values can be written in.
 *
 * <p>The layouts frame the same values differently and none of them is self-describing, so decoding one as another
 * returns other bytes rather than failing. Every reader therefore has to be given the layout the values were
 * written in, and a reader that names the wrong one answers quietly and wrongly. Two readers are covered here: the
 * queries a field with no inverted index answers from its doc values, and, for a field type that stores no
 * positions, the phrases confirmed by reading the values back and analysing them again.
 *
 * <p>Layouts are reached through the mapping rather than named directly, so a field type that stops writing one, or
 * starts writing another, fails {@link #testEveryLayoutIsCovered} rather than leaving a layout nobody reads back.
 *
 * <p>Fielddata over these layouts is covered by {@link AbstractColumnarArrayOrderFieldDataTestCase}.
 */
public abstract class AbstractColumnarBinaryLayoutTestCase extends MapperServiceTestCase {

    protected static final String FIELD = "field";

    /**
     * How a field's values are framed on disk, with the mapping that gets them written that way. A columnar index
     * writes {@link BinaryDocValuesFormat#ARRAY_ORDER_INLINE_NULL} for a field that keeps array order and
     * {@link BinaryDocValuesFormat#SEPARATE_COUNT} for one that cannot hold an array to begin with; turning the
     * ColumNAR codec on replaces both with {@link BinaryDocValuesFormat#COLUMNAR_PAYLOAD}.
     */
    protected enum Layout {
        SEPARATE_COUNT(BinaryDocValuesFormat.SEPARATE_COUNT, false, false),
        ARRAY_ORDER_INLINE_NULL(BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL, true, false),
        COLUMNAR_PAYLOAD(BinaryDocValuesFormat.COLUMNAR_PAYLOAD, true, true);

        private final BinaryDocValuesFormat format;
        private final boolean multiValue;
        private final boolean codec;

        Layout(BinaryDocValuesFormat format, boolean multiValue, boolean codec) {
            this.format = format;
            this.multiValue = multiValue;
            this.codec = codec;
        }

        Settings settings() {
            final Settings.Builder settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName());
            if (ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled()) {
                // Named on every layout rather than left to the default, so that the two layouts the stock codec
                // writes keep being written once the ColumNAR codec is the default. The setting only exists while
                // the flag does, and a build without it writes neither the codec's layout nor this setting.
                settings.put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), codec);
            }
            return settings.build();
        }

        /** Whether this build writes this layout at all, which for the ColumNAR codec is its feature flag. */
        boolean isAvailable() {
            return codec == false || ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled();
        }
    }

    /** What a query run over {@link #documents} is expected to match, worked out from the documents themselves. */
    protected static final class Hits {

        private final Layout layout;
        private final IndexSearcher searcher;
        private final List<List<String>> documents;

        private Hits(Layout layout, IndexSearcher searcher, List<List<String>> documents) {
            this.layout = layout;
            this.searcher = searcher;
            this.documents = documents;
        }

        /**
         * Asserts the query returns exactly the documents holding a value {@code matches} accepts. Compared as the
         * documents themselves rather than how many there are, so a decoder that reads other bytes cannot pass by
         * returning as many of the wrong ones.
         */
        public void assertMatches(Query query, Predicate<String> matches) throws IOException {
            final List<Integer> expected = new ArrayList<>();
            for (int doc = 0; doc < documents.size(); doc++) {
                for (String value : documents.get(doc)) {
                    if (value != null && matches.test(value)) {
                        expected.add(doc);
                        break;
                    }
                }
            }
            final List<Integer> actual = new ArrayList<>();
            for (ScoreDoc hit : searcher.search(query, Math.max(1, documents.size())).scoreDocs) {
                actual.add(hit.doc);
            }
            Collections.sort(actual);
            assertEquals(layout + " " + query, expected, actual);
        }
    }

    /** The body of a test that runs against one layout. */
    protected interface LayoutBody {
        void accept(MappedFieldType field, SearchExecutionContext context, Hits hits) throws IOException;
    }

    protected abstract String fieldTypeName();

    /** The layout this field's values were written in, which each field type reports on its own field type. */
    protected abstract BinaryDocValuesFormat binaryFormatOf(MappedFieldType fieldType);

    /**
     * Whether this field type confirms a phrase by reading its values back and analysing them again, which is what a
     * field type that stores no positions has to do. Those reads go through a layout, so they are covered here. A
     * field type that phrases from positions never reads the column for it and does not answer this.
     */
    protected boolean confirmsPhrasesFromValues() {
        return false;
    }

    /**
     * The documents the query tests run over. A layout that keeps array order is given arrays with a null, a
     * repeat and an all-null document in them, since those are what the layouts frame differently; the
     * single-valued layout is given the same values one to a document.
     */
    private static List<List<String>> documents(Layout layout) {
        if (layout.multiValue) {
            return List.of(
                Arrays.asList("alpha", "beta"),
                List.of("gamma"),
                Arrays.asList("alpha", null, "alpha"),
                Arrays.asList((String) null, null),
                List.of(),
                List.of("delta")
            );
        }
        return List.of(List.of("alpha"), List.of("gamma"), List.of("alpha"), List.of(), List.of(), List.of("delta"));
    }

    /**
     * The documents the phrase tests run over. The value the phrase is in is not the document's first, so a
     * decoder that frames the blob wrongly reads something other than it rather than happening to read it anyway.
     */
    private static List<List<String>> phraseDocuments(Layout layout) {
        if (layout.multiValue) {
            return List.of(Arrays.asList("lazy dog", null, "the quick brown fox"), List.of("nothing to see here"));
        }
        return List.of(List.of("the quick brown fox"), List.of("nothing to see here"));
    }

    public void testEveryLayoutIsCovered() throws IOException {
        final EnumSet<BinaryDocValuesFormat> covered = EnumSet.noneOf(BinaryDocValuesFormat.class);
        for (Layout layout : Layout.values()) {
            if (layout.isAvailable()) {
                assertEquals(layout.toString(), layout.format, binaryFormatOf(mapperService(layout, false).fieldType(FIELD)));
            }
            // A layout this build does not write counts as covered: it cannot be reached here and cannot be written in
            // production either, so the mapping that would reach it is checked wherever the build does write it.
            covered.add(layout.format);
        }
        assertEquals("every layout needs a mapping that reaches it", EnumSet.allOf(BinaryDocValuesFormat.class), covered);
    }

    /**
     * The queries every field type here answers from its doc values once it has no inverted index. Each decodes the
     * values itself, so each has to be given the layout they were written in.
     */
    public void testQueriesAreAnsweredFromTheColumn() throws IOException {
        forEachLayout((field, context, hits) -> {
            hits.assertMatches(field.termQuery("alpha", context), "alpha"::equals);
            hits.assertMatches(
                field.termsQuery(List.of("alpha", "gamma"), context),
                value -> value.equals("alpha") || value.equals("gamma")
            );
            hits.assertMatches(field.prefixQuery("al", null, false, context), value -> value.startsWith("al"));
            hits.assertMatches(field.wildcardQuery("g*a", null, false, context), value -> value.matches("g.*a"));
            hits.assertMatches(
                field.regexpQuery("al.*", 0, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null, context),
                value -> value.matches("al.*")
            );
        });
    }

    /**
     * The values a phrase is confirmed against are read through the layout they were written in. Getting that wrong
     * does not fail: the phrase matches nothing, and so do phrase prefix and intervals, which read them the same way.
     */
    public void testPhrasesAreConfirmedAgainstTheColumn() throws IOException {
        assumeTrue("field type confirms phrases from positions, not from its values", confirmsPhrasesFromValues());
        int ran = 0;
        for (Layout layout : Layout.values()) {
            if (layout.isAvailable() == false) {
                continue;
            }
            final MapperService mapperService = mapperService(layout, true);
            assertEquals(layout.toString(), layout.format, binaryFormatOf(mapperService.fieldType(FIELD)));
            withSearcher(mapperService, phraseDocuments(layout), (searcher, context) -> {
                assertEquals(layout + " phrase", 1, searcher.count(new MatchPhraseQueryBuilder(FIELD, "quick brown").toQuery(context)));
                assertEquals(
                    layout + " phrase prefix",
                    1,
                    searcher.count(new MatchPhrasePrefixQueryBuilder(FIELD, "quick bro").toQuery(context))
                );
                assertEquals(
                    layout + " intervals",
                    1,
                    searcher.count(
                        new IntervalQueryBuilder(FIELD, new IntervalsSourceProvider.Match("quick brown", 0, true, null, null, null))
                            .toQuery(context)
                    )
                );
                assertEquals(
                    layout + " a phrase that is not there",
                    0,
                    searcher.count(new MatchPhraseQueryBuilder(FIELD, "brown quick").toQuery(context))
                );
            });
            ran++;
        }
        assertThat("no layout could be reached", ran, greaterThan(0));
    }

    /**
     * Runs {@code body} once per layout this build writes, over a field with no inverted index so that its queries
     * are answered from its doc values. Fails if no layout could be reached at all, so a build that writes none of
     * them is not read as coverage.
     */
    protected void forEachLayout(LayoutBody body) throws IOException {
        int ran = 0;
        for (Layout layout : Layout.values()) {
            if (layout.isAvailable() == false) {
                continue;
            }
            final MapperService mapperService = mapperService(layout, false);
            assertEquals(layout.toString(), layout.format, binaryFormatOf(mapperService.fieldType(FIELD)));
            final List<List<String>> documents = documents(layout);
            withSearcher(
                mapperService,
                documents,
                (searcher, context) -> body.accept(mapperService.fieldType(FIELD), context, new Hits(layout, searcher, documents))
            );
            ran++;
        }
        assertThat("no layout could be reached", ran, greaterThan(0));
    }

    /** The mapping that reaches {@code layout}, with or without an inverted index to answer queries from. */
    protected MapperService mapperService(Layout layout, boolean indexed) throws IOException {
        return createMapperService(layout.settings(), mapping(b -> {
            b.startObject(FIELD).field("type", fieldTypeName());
            if (indexed == false) {
                b.field("index", false);
            }
            if (layout.multiValue) {
                b.field("doc_values", true);
            } else {
                b.startObject("doc_values").field("multi_value", false).endObject();
            }
            b.endObject();
        }));
    }

    private void withSearcher(MapperService mapperService, List<List<String>> documents, CheckedSearcherConsumer body) throws IOException {
        withLuceneIndex(mapperService, iw -> {
            for (List<String> values : documents) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> writeValues(b, values))).rootDoc());
            }
        }, reader -> body.accept(new IndexSearcher(reader), createSearchExecutionContext(mapperService, newSearcher(reader))));
    }

    private interface CheckedSearcherConsumer {
        void accept(IndexSearcher searcher, SearchExecutionContext context) throws IOException;
    }

    private static void writeValues(XContentBuilder b, List<String> values) throws IOException {
        if (values.isEmpty()) {
            return;
        }
        if (values.size() == 1) {
            b.field(FIELD, values.get(0));
            return;
        }
        final List<String> copy = new ArrayList<>(values);
        b.field(FIELD, copy);
    }
}
