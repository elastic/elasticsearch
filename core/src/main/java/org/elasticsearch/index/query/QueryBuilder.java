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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

public interface QueryBuilder<QB extends QueryBuilder<QB>> extends NamedWriteable<QB>, ToXContent {

    /**
     * Converts this QueryBuilder to a lucene {@link Query}.
     * Returns <tt>null</tt> if this query should be ignored in the context of
     * parent queries.
     *
     * @param context additional information needed to construct the queries
     * @return the {@link Query} or <tt>null</tt> if this query should be ignored upstream
     */
    Query toQuery(QueryShardContext context) throws IOException;

    /**
     * Converts this QueryBuilder to an unscored lucene {@link Query} that acts as a filter.
     * Returns <tt>null</tt> if this query should be ignored in the context of
     * parent queries.
     *
     * @param context additional information needed to construct the queries
     * @return the {@link Query} or <tt>null</tt> if this query should be ignored upstream
     */
    Query toFilter(QueryShardContext context) throws IOException;

    /**
     * Sets the arbitrary name to be assigned to the query (see named queries).
     */
    QB queryName(String queryName);

    /**
     * Returns the arbitrary name assigned to the query (see named queries).
     */
    String queryName();

    /**
     * Returns the boost for this query.
     */
    float boost();

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    QB boost(float boost);

    /**
     * Returns the name that identifies uniquely the query
     */
    String getName();

    /**
     * Rewrites this query builder into its primitive form. By default this method return the builder itself. If the builder
     * did not change the identity reference must be returned otherwise the builder will be rewritten infinitely.
     */
    default QueryBuilder<?> rewrite(QueryRewriteContext queryShardContext) throws IOException {
        return this;
    }

    /**
     * Rewrites the given query into its primitive form. Queries that for instance fetch resources from remote hosts or
     * can simplify / optimize itself should do their heavy lifting during {@link #rewrite(QueryRewriteContext)}. This method
     * rewrites the query until it doesn't change anymore.
     * @throws IOException if an {@link IOException} occurs
     */
    static QueryBuilder<?> rewriteQuery(QueryBuilder<?> original, QueryRewriteContext context) throws IOException {
        QueryBuilder builder = original;
        for (QueryBuilder rewrittenBuilder = builder.rewrite(context); rewrittenBuilder != builder;
             rewrittenBuilder = builder.rewrite(context)) {
            builder = rewrittenBuilder;
        }
        return builder;
    }

    /**
     * Names under which the QueryBuilder is registered. QueryBuilders should have a public static final NAMES member declaring all the
     * names that the query uses.
     */
    public static class Names {
        private final String primary;
        private final Set<String> all;

        /**
         * Names under which the QueryBuilder is registered.
         *
         * @param primary this is the name that the query builder outputs with
         *        {@link QueryBuilder#toXContent(XContentBuilder, org.elasticsearch.common.xcontent.ToXContent.Params)} and that it uses as
         *        it's {@link QueryBuilder#getWriteableName()}.
         * @param alternatives names that can be used for the query in XContent. Sometimes these are just deprecated names for the same
         *        query like <code>multiMatch</code> is a deprecated name for <code>multi_match</code>. Sometimes these alternate forms that
         *        effect parsing like <code>match_phrase</code> is just a <code>match</code> query with some settings tweaked.
         */
        public Names(String primary, String... alternatives) {
            this.primary = primary;
            Set<String> all = new HashSet<>(alternatives.length + 1);
            all.add(primary);
            for (String alternative: alternatives) {
                boolean added = all.add(alternative);
                if (false == added) {
                    throw new IllegalArgumentException(
                            "Alternative name [" + alternative + "] is listed twice or is the same as a primary name");
                }
            }
            this.all = unmodifiableSet(all);
        }

        /**
         * Fetch the primary name of this query. This is the name that the query builder outputs with
         * {@link QueryBuilder#toXContent(XContentBuilder, org.elasticsearch.common.xcontent.ToXContent.Params)} and that it
         * uses as it's {@link QueryBuilder#getWriteableName()}.
         */
        public String primary() {
            return primary;
        }

        /**
         * All names the might refer to this query builder in XContent. Sometimes these are just deprecated names for the same query like
         * <code>multiMatch</code> is a deprecated name for <code>multi_match</code>. Sometimes these alternate forms that effect parsing
         * like <code>match_phrase</code> is just a <code>match</code> query with some settings tweaked.
         */
        public Set<String> all() {
            return all;
        }
    }
}
