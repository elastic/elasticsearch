/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;

/** The combination of queries matches the documents that all of them match. */
public class QueriesMatchTests extends MapperServiceTestCase {

    private static final List<String> FIELDS = List.of("a", "b", "c");

    public void testCombinationMatchesEveryQuery() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            for (String field : FIELDS) {
                b.startObject(field).field("type", "long").endObject();
            }
        }));
        withLuceneIndex(mapperService, iw -> {
            for (int a = 0; a < 3; a++) {
                for (int b = 0; b < 3; b++) {
                    for (int c = 0; c < 3; c++) {
                        int va = a, vb = b, vc = c;
                        iw.addDocument(
                            mapperService.documentMapper().parse(source(d -> d.field("a", va).field("b", vb).field("c", vc))).rootDoc()
                        );
                    }
                }
            }
        }, reader -> {
            SearchExecutionContext context = createSearchExecutionContext(mapperService, new IndexSearcher(reader));
            for (int i = 0; i < 200; i++) {
                List<QueryBuilder> queries = new ArrayList<>();
                for (int q = between(1, 4); q > 0; q--) {
                    queries.add(randomBoolean() ? null : randomQuery(2));
                }
                Queries.Clause clause = randomFrom(Queries.Clause.values());
                QueryBuilder combination = Queries.combine(clause, queries);

                Set<Integer> expected = null;
                for (QueryBuilder query : queries) {
                    if (query != null) {
                        Set<Integer> matches = matches(reader, context, query);
                        if (expected == null) {
                            expected = matches;
                        } else {
                            expected.retainAll(matches);
                        }
                    }
                }
                if (expected == null) {
                    assertNull(combination);
                } else {
                    assertThat(clause + " " + queries, matches(reader, context, combination), equalTo(expected));
                }
            }
        });
    }

    private static QueryBuilder randomQuery(int depth) {
        if (depth == 0 || randomBoolean()) {
            return QueryBuilders.termQuery(randomFrom(FIELDS), between(0, 2));
        }
        var bool = QueryBuilders.boolQuery();
        switch (between(0, 3)) {
            case 0 -> bool.should(randomQuery(depth - 1)).should(randomQuery(depth - 1));
            case 1 -> bool.filter(randomQuery(depth - 1)).filter(randomQuery(depth - 1));
            case 2 -> bool.must(randomQuery(depth - 1)).mustNot(randomQuery(depth - 1));
            case 3 -> bool.should(randomQuery(depth - 1)).should(randomQuery(depth - 1)).filter(randomQuery(depth - 1));
            default -> throw new AssertionError();
        }
        return bool;
    }

    private static Set<Integer> matches(DirectoryReader reader, SearchExecutionContext context, QueryBuilder query) throws IOException {
        Set<Integer> docs = new TreeSet<>();
        for (ScoreDoc doc : new IndexSearcher(reader).search(query.toQuery(context), reader.maxDoc() + 1).scoreDocs) {
            docs.add(doc.doc);
        }
        return docs;
    }
}
