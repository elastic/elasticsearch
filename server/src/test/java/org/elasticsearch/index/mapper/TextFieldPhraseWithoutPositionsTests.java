/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.IntervalQueryBuilder;
import org.elasticsearch.index.query.IntervalsSourceProvider;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@code text} field indexing no positions answers the queries that ask about them by confirming against its own
 * values. Each asks the same question of the same mapping with positions, which is the answer to match.
 */
public class TextFieldPhraseWithoutPositionsTests extends MapperServiceTestCase {

    private static final List<Object> DOCS = List.of(
        "the quick brown fox jumps",
        "brown the quick fox",
        "quick brown",
        List.of("ends with quick", "brown starts here"),
        ""
    );

    private MapperService mapper(String indexOptions) throws IOException {
        return createMapperService(
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
            mapping(b -> b.startObject("body").field("type", "text").field("index_options", indexOptions).endObject())
        );
    }

    public void testIndexOptionsAreWhatTheMappingAsksFor() throws IOException {
        assertEquals(
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
            mapper("positions").fieldType("body").getTextSearchInfo().luceneFieldType().indexOptions()
        );
        assertEquals(IndexOptions.DOCS, mapper("docs").fieldType("body").getTextSearchInfo().luceneFieldType().indexOptions());
    }

    public void testPhrasesAndIntervalsMatchTheSameDocuments() throws IOException {
        final List<QueryBuilder> queries = new ArrayList<>();
        for (String phrase : List.of("quick brown", "brown fox", "the quick brown fox", "fox the", "quick brown fox")) {
            queries.add(new MatchPhraseQueryBuilder("body", phrase));
        }
        queries.add(new MatchPhraseQueryBuilder("body", "quick fox").slop(1));
        queries.add(new MatchPhraseQueryBuilder("body", "quick fox").slop(2));
        for (String phrase : List.of("quick bro", "the quick brown f", "fox th")) {
            queries.add(new MatchPhrasePrefixQueryBuilder("body", phrase));
        }
        queries.add(intervals(new IntervalsSourceProvider.Match("quick brown", 0, true, null, null, null)));
        queries.add(intervals(new IntervalsSourceProvider.Match("quick fox", 1, true, null, null, null)));
        queries.add(intervals(new IntervalsSourceProvider.Prefix("bro", null, null)));
        queries.add(intervals(new IntervalsSourceProvider.Wildcard("qu*ck", null, null)));
        queries.add(intervals(new IntervalsSourceProvider.Regexp("q.*k", null, null)));
        queries.add(intervals(new IntervalsSourceProvider.Fuzzy("quikc", 0, true, Fuzziness.AUTO, null, null)));
        queries.add(intervals(new IntervalsSourceProvider.Range("brown", "fox", true, true, null, null)));

        final List<List<Integer>> withPositions = matching(mapper("positions"), queries);
        final List<List<Integer>> withoutPositions = matching(mapper("docs"), queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), withPositions.get(q), withoutPositions.get(q));
        }
        // The fourth document holds two values and the phrase spans them, which both sides do alike: the
        // confirmation carries positions from one value into the next, as the index it stands in for does.
        assertEquals("a phrase spans two values", List.of(0, 2, 3), withoutPositions.get(0));
        logger.info("{} queries agreed with and without positions", queries.size());
    }

    /** Outside strict columnar a text field has no doc values, so there is nothing to confirm against and it refuses. */
    public void testWithoutDocValuesItStillRefuses() throws IOException {
        final SearchExecutionContext context = createSearchExecutionContext(
            createMapperService(mapping(b -> b.startObject("body").field("type", "text").field("index_options", "docs").endObject()))
        );
        assertEquals(
            "field:[body] was indexed without position data; cannot run PhraseQuery",
            expectThrows(IllegalArgumentException.class, () -> new MatchPhraseQueryBuilder("body", "quick brown").toQuery(context))
                .getMessage()
        );
        assertEquals(
            "Cannot create intervals over field [body] with no positions indexed",
            expectThrows(
                IllegalArgumentException.class,
                () -> intervals(new IntervalsSourceProvider.Match("quick brown", 0, true, null, null, null)).toQuery(context)
            ).getMessage()
        );
    }

    private static QueryBuilder intervals(IntervalsSourceProvider source) {
        return new IntervalQueryBuilder("body", source);
    }

    /** What each of {@code queries} matches, over one index built from {@link #DOCS}. */
    private List<List<Integer>> matching(MapperService mapperService, List<QueryBuilder> queries) throws IOException {
        final List<List<Integer>> perQuery = new ArrayList<>();
        withLuceneIndex(mapperService, iw -> {
            for (Object doc : DOCS) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("body", doc))).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            final IndexSearcher searcher = newSearcher(reader);
            for (QueryBuilder query : queries) {
                final List<Integer> hits = new ArrayList<>();
                for (var hit : searcher.search(query.toQuery(context), 10).scoreDocs) {
                    hits.add(hit.doc);
                }
                Collections.sort(hits);
                perQuery.add(hits);
            }
        });
        return perQuery;
    }
}
