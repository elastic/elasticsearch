/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;

import java.io.IOException;
import java.util.Objects;

/** A {@link ToParentBlockJoinQuery} that allows to retrieve its nested path. */
public final class ESToParentBlockJoinQuery extends Query {

    private final ToParentBlockJoinQuery query;
    private final String path;
    private final ScoreMode scoreMode;

    public ESToParentBlockJoinQuery(Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode, String path) {
        this(new ToParentBlockJoinQuery(childQuery, parentsFilter, scoreMode), path, scoreMode);
    }

    private ESToParentBlockJoinQuery(ToParentBlockJoinQuery query, String path, ScoreMode scoreMode) {
        this.query = query;
        this.path = path;
        this.scoreMode = scoreMode;
    }

    /** Return the child query. */
    public Query getChildQuery() {
        return query.getChildQuery();
    }

    /** Return the path of results of this query, or {@code null} if matches are at the root level. */
    public String getPath() {
        return path;
    }

    /** Return the score mode for the matching children. **/
    public ScoreMode getScoreMode() {
        return scoreMode;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query innerRewrite = query.rewrite(searcher);
        if (innerRewrite != query) {
            // Right now ToParentBlockJoinQuery always rewrites to a ToParentBlockJoinQuery
            // so the else block will never be used. It is useful in the case that
            // ToParentBlockJoinQuery one day starts to rewrite to a different query, eg.
            // a MatchNoDocsQuery if it realizes that it cannot match any docs and rewrites
            // to a MatchNoDocsQuery. In that case it would be fine to lose information
            // about the nested path.
            if (innerRewrite instanceof ToParentBlockJoinQuery) {
                return new ESToParentBlockJoinQuery((ToParentBlockJoinQuery) innerRewrite, path, scoreMode);
            } else {
                return innerRewrite;
            }
        }
        return super.rewrite(searcher);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        // Highlighters must visit the child query to extract terms
        query.getChildQuery().visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost) throws IOException {
        return query.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        ESToParentBlockJoinQuery that = (ESToParentBlockJoinQuery) obj;
        return query.equals(that.query) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), query, path);
    }

    @Override
    public String toString(String field) {
        return query.toString(field);
    }

}
