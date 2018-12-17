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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;

/**
 * Marker interface for a specific type of {@link QueryBuilder} that allows to build span queries.
 */
public interface SpanQueryBuilder extends QueryBuilder {

    class SpanQueryBuilderUtil {

        private static final DeprecationLogger DEPRECATION_LOGGER = 
            new DeprecationLogger(LogManager.getLogger(SpanQueryBuilderUtil.class));

        private SpanQueryBuilderUtil() {
            // utility class
        }

        /**
         * Checks boost value of a nested span clause is equal to {@link AbstractQueryBuilder#DEFAULT_BOOST},
         * and if not issues a deprecation warning
         * @param clause    a span query builder
         */
        static void checkNoBoost(SpanQueryBuilder clause) {
            try {
                if (clause.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
                    DEPRECATION_LOGGER.deprecatedAndMaybeLog("span_inner_queries", "setting boost on inner span queries is deprecated!");
                }
            } catch (UnsupportedOperationException ignored) {
                // if boost is unsupported it can't have been set
            }
        }
    }
}
