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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;

/**
 * Marker interface for a specific type of {@link QueryBuilder} that allows to build span queries.
 */
public interface SpanQueryBuilder extends QueryBuilder {

    class SpanQueryBuilderUtil {
        private SpanQueryBuilderUtil() {
            // utility class
        }

        /**
         * Checks boost value of a nested span clause is equal to {@link AbstractQueryBuilder#DEFAULT_BOOST}.
         *
         * @param queryName a query name
         * @param fieldName a field name
         * @param parser    a parser
         * @param clause    a span query builder
         * @throws ParsingException if query boost value isn't equal to {@link AbstractQueryBuilder#DEFAULT_BOOST}
         */
        static void checkNoBoost(String queryName, String fieldName, XContentParser parser, SpanQueryBuilder clause) {
            try {
                if (clause.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
                    throw new ParsingException(parser.getTokenLocation(), queryName + " [" + fieldName + "] " +
                        "as a nested span clause can't have non-default boost value [" + clause.boost() + "]");
                }
            } catch (UnsupportedOperationException ignored) {
                // if boost is unsupported it can't have been set
            }
        }
    }
}
