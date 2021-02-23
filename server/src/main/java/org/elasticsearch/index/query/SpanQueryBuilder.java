/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
