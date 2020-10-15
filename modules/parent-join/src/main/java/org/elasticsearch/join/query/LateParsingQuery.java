/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.join.query;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that rewrites into another query using
 * {@link JoinUtil#createJoinQuery(String, Query, Query, IndexSearcher, ScoreMode, OrdinalMap, int, int)}
 * that executes the actual join.
 * <p>
 * This query is exclusively used by the {@link HasChildQueryBuilder} and {@link HasParentQueryBuilder} to get access
 * to the {@link DirectoryReader} used by the current search in order to retrieve the {@link OrdinalMap}.
 * The {@link OrdinalMap} is required by {@link JoinUtil} to execute the join.
 */
// TODO: Find a way to remove this query and let doToQuery(...) just return the query from JoinUtil.createJoinQuery(...)
public final class LateParsingQuery extends Query {

    private final Query toQuery;
    private final Query innerQuery;
    private final int minChildren;
    private final int maxChildren;
    private final String joinField;
    private final ScoreMode scoreMode;
    private final SortedSetOrdinalsIndexFieldData fieldDataJoin;
    private final Similarity similarity;

    LateParsingQuery(Query toQuery, Query innerQuery, int minChildren, int maxChildren,
                     String joinField, ScoreMode scoreMode,
                     SortedSetOrdinalsIndexFieldData fieldData, Similarity similarity) {
        this.toQuery = toQuery;
        this.innerQuery = innerQuery;
        this.minChildren = minChildren;
        this.maxChildren = maxChildren;
        this.joinField = joinField;
        this.scoreMode = scoreMode;
        this.fieldDataJoin = fieldData;
        this.similarity = similarity;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = super.rewrite(reader);
        if (rewritten != this) {
            return rewritten;
        }
        if (reader instanceof DirectoryReader) {
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            indexSearcher.setQueryCache(null);
            indexSearcher.setSimilarity(similarity);
            IndexOrdinalsFieldData indexParentChildFieldData = fieldDataJoin.loadGlobal((DirectoryReader) reader);
            OrdinalMap ordinalMap = indexParentChildFieldData.getOrdinalMap();
            return JoinUtil.createJoinQuery(joinField, innerQuery, toQuery, indexSearcher, scoreMode,
                ordinalMap, minChildren, maxChildren);
        } else {
            if (reader.leaves().isEmpty() && reader.numDocs() == 0) {
                // asserting reader passes down a MultiReader during rewrite which makes this
                // blow up since for this query to work we have to have a DirectoryReader otherwise
                // we can't load global ordinals - for this to work we simply check if the reader has no leaves
                // and rewrite to match nothing
                return new MatchNoDocsQuery();
            }
            throw new IllegalStateException("can't load global ordinals for reader of type: " +
                reader.getClass() + " must be a DirectoryReader");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (sameClassAs(o) == false) return false;

        LateParsingQuery that = (LateParsingQuery) o;

        if (minChildren != that.minChildren) return false;
        if (maxChildren != that.maxChildren) return false;
        if (!toQuery.equals(that.toQuery)) return false;
        if (!innerQuery.equals(that.innerQuery)) return false;
        if (!joinField.equals(that.joinField)) return false;
        return scoreMode == that.scoreMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), toQuery, innerQuery, minChildren, maxChildren, joinField, scoreMode);
    }

    @Override
    public String toString(String s) {
        return "LateParsingQuery {joinField=" + joinField + "}";
    }

    public int getMinChildren() {
        return minChildren;
    }

    public int getMaxChildren() {
        return maxChildren;
    }

    public ScoreMode getScoreMode() {
        return scoreMode;
    }

    public Query getInnerQuery() {
        return innerQuery;
    }

    public Similarity getSimilarity() {
        return similarity;
    }
}
