/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.IntervalQueryBuilder;
import org.elasticsearch.index.query.IntervalsSourceProvider;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@code text} field indexing no positions answers the queries that ask about them by confirming against its own
 * values. Each asks the same question of the same mapping with positions, which is the answer to match.
 */
public class TextFieldPhraseWithoutPositionsTests extends MapperServiceTestCase {

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            // The gap a text field's analyzer carries in an index, which sits between two values of one document.
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer(), TextFieldMapper.Defaults.POSITION_INCREMENT_GAP)
            ),
            Map.of("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())),
            Map.of()
        );
    }

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
        // The fourth document holds "quick" at the end of one value and "brown" at the start of the next, which
        // the gap between them keeps apart on both sides.
        assertEquals("a phrase does not span two values", List.of(0, 2), withoutPositions.get(0));
        logger.info("{} queries agreed with and without positions", queries.size());
    }

    /** With no values to confirm against, outside strict columnar or with doc values off, it refuses as it always has. */
    public void testWithoutValuesToConfirmAgainstItRefuses() throws IOException {
        assertRefuses(
            createMapperService(mapping(b -> b.startObject("body").field("type", "text").field("index_options", "docs").endObject()))
        );
        assertRefuses(
            createMapperService(
                Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
                mapping(
                    b -> b.startObject("body").field("type", "text").field("index_options", "docs").field("doc_values", false).endObject()
                )
            )
        );
    }

    /** A parent with a normalizer keeps rewritten values, so a multi-field reading them would answer wrongly. */
    public void testParentWithANormalizerIsNotRead() throws IOException {
        assertRefuses(
            createMapperService(
                Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
                mapping(
                    b -> b.startObject("host")
                        .field("type", "keyword")
                        .field("normalizer", "lowercase")
                        .startObject("fields")
                        .startObject("body")
                        .field("type", "text")
                        .field("index_options", "docs")
                        .field("doc_values", false)
                        .endObject()
                        .endObject()
                        .endObject()
                )
            ),
            "host.body"
        );
    }

    private void assertRefuses(MapperService mapperService) {
        assertRefuses(mapperService, "body");
    }

    private void assertRefuses(MapperService mapperService, String field) {
        final SearchExecutionContext context = createSearchExecutionContext(mapperService);
        assertEquals(
            "field:[" + field + "] was indexed without position data; cannot run PhraseQuery",
            expectThrows(IllegalArgumentException.class, () -> new MatchPhraseQueryBuilder(field, "quick brown").toQuery(context))
                .getMessage()
        );
        assertEquals(
            "Cannot create intervals over field [" + field + "] with no positions indexed",
            expectThrows(
                IllegalArgumentException.class,
                () -> new IntervalQueryBuilder(field, new IntervalsSourceProvider.Match("quick brown", 0, true, null, null, null)).toQuery(
                    context
                )
            ).getMessage()
        );
    }

    /** A text multi-field confirms against its own values, or its parent's where it was told to keep none. */
    public void testMultiField() throws IOException {
        for (boolean ownDocValues : List.of(true, false)) {
            for (String options : List.of("positions", "docs")) {
                assertMultiFieldMatches(options, ownDocValues);
            }
        }
    }

    private void assertMultiFieldMatches(String options, boolean ownDocValues) throws IOException {
        final MapperService mapper = createMapperService(
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
            mapping(
                b -> b.startObject("host")
                    .field("type", "keyword")
                    .startObject("fields")
                    .startObject("text")
                    .field("type", "text")
                    .field("index_options", options)
                    .field("doc_values", ownDocValues)
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        final List<Integer> hits = new ArrayList<>();
        withLuceneIndex(mapper, iw -> {
            for (String value : List.of("quick brown fox", "brown quick")) {
                iw.addDocument(mapper.documentMapper().parse(source(b -> b.field("host", value))).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapper);
            for (var hit : newSearcher(reader).search(
                new MatchPhraseQueryBuilder("host.text", "quick brown").toQuery(context),
                10
            ).scoreDocs) {
                hits.add(hit.doc);
            }
        });
        assertEquals("index_options [" + options + "] doc_values [" + ownDocValues + "]", List.of(0), hits);
    }

    /** A text field inside a nested object is read on the nested document holding it. */
    public void testNested() throws IOException {
        for (String options : List.of("positions", "docs")) {
            final MapperService mapper = createMapperService(
                Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
                mapping(
                    b -> b.startObject("entries")
                        .field("type", "nested")
                        .startObject("properties")
                        .startObject("body")
                        .field("type", "text")
                        .field("index_options", options)
                        .endObject()
                        .endObject()
                        .endObject()
                )
            );
            final List<Integer> hits = new ArrayList<>();
            withLuceneIndex(mapper, iw -> {
                // One parent holding two entries, only the second of which has the phrase.
                iw.addDocuments(mapper.documentMapper().parse(source(b -> {
                    b.startArray("entries");
                    b.startObject().field("body", "brown quick").endObject();
                    b.startObject().field("body", "quick brown fox").endObject();
                    b.endArray();
                })).docs());
            }, reader -> {
                final SearchExecutionContext context = createSearchExecutionContext(mapper);
                final QueryBuilder nested = new NestedQueryBuilder(
                    "entries",
                    new MatchPhraseQueryBuilder("entries.body", "quick brown"),
                    ScoreMode.Avg
                );
                for (var hit : newSearcher(wrapInMockESDirectoryReader(reader)).search(nested.toQuery(context), 10).scoreDocs) {
                    hits.add(hit.doc);
                }
            });
            assertEquals("one parent matches, index_options [" + options + "]", 1, hits.size());
        }
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
