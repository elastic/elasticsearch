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

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.query.QueryShardContext;

import java.util.List;

/**
 * A {@link MappedFieldType} that has the same value for all documents.
 * Factory methods for queries are called at rewrite time so they should be
 * cheap. In particular they should not read data from disk or perform a
 * network call. Furthermore they may only return a {@link MatchAllDocsQuery}
 * or a {@link MatchNoDocsQuery}.
 */
public abstract class ConstantFieldType extends MappedFieldType {

    public ConstantFieldType() {
        super();
    }

    public ConstantFieldType(ConstantFieldType other) {
        super(other);
    }

    @Override
    public final boolean isSearchable() {
        return true;
    }

    @Override
    public final boolean isAggregatable() {
        return true;
    }

    /**
     * Return whether the constant value of this field matches the provided {@code pattern}
     * as documented in {@link Regex#simpleMatch}.
     */
    protected abstract boolean matches(String pattern, QueryShardContext context);

    private static String valueToString(Object value) {
        return value instanceof BytesRef
                ? ((BytesRef) value).utf8ToString()
                : value.toString();
    }

    @Override
    public final Query termQuery(Object value, QueryShardContext context) {
        String pattern = valueToString(value);
        if (matches(pattern, context)) {
            return Queries.newMatchAllQuery();
        } else {
            return new MatchNoDocsQuery();
        }
    }

    @Override
    public final Query termsQuery(List<?> values, QueryShardContext context) {
        for (Object value : values) {
            String pattern = valueToString(value);
            if (matches(pattern, context)) {
                // `terms` queries are a disjunction, so one matching term is enough
                return Queries.newMatchAllQuery();
            }
        }
        return new MatchNoDocsQuery();
    }

    @Override
    public final Query prefixQuery(String prefix,
                             @Nullable MultiTermQuery.RewriteMethod method,
                             QueryShardContext context) {
        String pattern = prefix + "*";
        if (matches(pattern, context)) {
            return Queries.newMatchAllQuery();
        } else {
            return new MatchNoDocsQuery();
        }
    }

    @Override
    public final Query wildcardQuery(String value,
                               @Nullable MultiTermQuery.RewriteMethod method,
                               QueryShardContext context) {
        if (matches(value, context)) {
            return Queries.newMatchAllQuery();
        } else {
            return new MatchNoDocsQuery();
        }
    }

}
