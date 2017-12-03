/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
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

    public ESToParentBlockJoinQuery(Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode, String path) {
        this(new ToParentBlockJoinQuery(childQuery, parentsFilter, scoreMode), path);
    }

    private ESToParentBlockJoinQuery(ToParentBlockJoinQuery query, String path) {
        this.query = query;
        this.path = path;
    }

    /** Return the child query. */
    public Query getChildQuery() {
        return query.getChildQuery();
    }

    /** Return the path of results of this query, or {@code null} if matches are at the root level. */
    public String getPath() {
        return path;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query innerRewrite = query.rewrite(reader);
        if (innerRewrite != query) {
            // Right now ToParentBlockJoinQuery always rewrites to a ToParentBlockJoinQuery
            // so the else block will never be used. It is useful in the case that
            // ToParentBlockJoinQuery one day starts to rewrite to a different query, eg.
            // a MatchNoDocsQuery if it realizes that it cannot match any docs and rewrites
            // to a MatchNoDocsQuery. In that case it would be fine to lose information
            // about the nested path.
            if (innerRewrite instanceof ToParentBlockJoinQuery) {
                return new ESToParentBlockJoinQuery((ToParentBlockJoinQuery) innerRewrite, path);
            } else {
                return innerRewrite;
            }
        }
        return super.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        return query.createWeight(searcher, needsScores, boost);
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
        return Objects.hash(getClass(), query, path);
    }

    @Override
    public String toString(String field) {
        return query.toString(field);
    }

}
