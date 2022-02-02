/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.io.IOException;
import java.util.List;

public class DummyQueryParserPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(DummyQueryBuilder.NAME, DummyQueryBuilder::new, DummyQueryBuilder::fromXContent),
            new QuerySpec<>(
                FailBeforeCurrentVersionQueryBuilder.NAME,
                FailBeforeCurrentVersionQueryBuilder::new,
                FailBeforeCurrentVersionQueryBuilder::fromXContent
            )
        );
    }

    public static class DummyQuery extends Query {
        private final Query matchAllDocsQuery = new MatchAllDocsQuery();

        @Override
        public String toString(String field) {
            return getClass().getSimpleName();
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return matchAllDocsQuery.createWeight(searcher, scoreMode, boost);
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj);
        }

        @Override
        public int hashCode() {
            return classHash();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }
    }
}
