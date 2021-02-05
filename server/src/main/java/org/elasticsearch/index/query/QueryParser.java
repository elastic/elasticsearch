/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Defines a query parser that is able to parse {@link QueryBuilder}s from {@link org.elasticsearch.common.xcontent.XContent}.
 */
@FunctionalInterface
public interface QueryParser<QB extends QueryBuilder> {
    /**
     * Creates a new {@link QueryBuilder} from the query held by the
     * {@link XContentParser}. The state on the parser contained in this context
     * will be changed as a side effect of this method call
     */
    QB fromXContent(XContentParser parser) throws IOException;
}
