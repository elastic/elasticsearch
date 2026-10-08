/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.MatchBoolPrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@code match_only_text} field of a strictly columnar index answers a text query from its values where it indexes no
 * terms, as the same mapping with an index does.
 */
public class MatchOnlyTextNoTermsSearchTests extends MapperServiceTestCase {

    private static final List<String> DOCS = List.of(
        "the quick brown fox jumps",
        "brown the quick fox",
        "quick brown",
        "nothing here at all"
    );

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    /** A boolean prefix holds a prefix clause the field wraps itself, beside the terms before it. */
    public void testBoolPrefixAnswersAsIndexed() throws IOException {
        final List<QueryBuilder> queries = List.of(
            new MatchBoolPrefixQueryBuilder("body", "bro"),
            new MatchBoolPrefixQueryBuilder("body", "quick bro"),
            new MatchBoolPrefixQueryBuilder("body", "the quick bro"),
            new MultiMatchQueryBuilder("quick bro", "body").type(MultiMatchQueryBuilder.Type.BOOL_PREFIX),
            new MatchPhrasePrefixQueryBuilder("body", "quick bro"),
            new MatchQueryBuilder("body", "quick nothing")
        );
        final List<List<Integer>> indexed = matching(true, queries);
        final List<List<Integer>> notIndexed = matching(false, queries);
        for (int q = 0; q < queries.size(); q++) {
            assertEquals(queries.get(q).toString(), indexed.get(q), notIndexed.get(q));
        }
    }

    private List<List<Integer>> matching(boolean indexed, List<QueryBuilder> queries) throws IOException {
        final MapperService mapperService = createMapperService(
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
            mapping(b -> b.startObject("body").field("type", "match_only_text").field("index", indexed).endObject())
        );
        final List<List<Integer>> perQuery = new ArrayList<>();
        withLuceneIndex(mapperService, iw -> {
            for (String doc : DOCS) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("body", doc))).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            final IndexSearcher searcher = newSearcher(reader);
            for (QueryBuilder query : queries) {
                final List<Integer> hits = new ArrayList<>();
                for (ScoreDoc hit : searcher.search(query.toQuery(context), 10).scoreDocs) {
                    hits.add(hit.doc);
                }
                hits.sort(null);
                perQuery.add(hits);
            }
        });
        return perQuery;
    }
}
