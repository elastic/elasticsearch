/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version on the datanode where the request is going to be
 * executed.
 *
 * Note: the way search requests are executed and rewritten currently on each node is that it is done by shard. So, `DataRewriteContext`
 * will be used in `rewrite` per shard but before the query phase.
 */
public class DataRewriteContext extends QueryRewriteContext {
    public DataRewriteContext(final XContentParserConfiguration parserConfiguration, final Client client, final LongSupplier nowInMillis) {
        super(parserConfiguration, client, nowInMillis);
    }

    @Override
    public DataRewriteContext convertToDataRewriteContext() {
        return this;
    }
}
