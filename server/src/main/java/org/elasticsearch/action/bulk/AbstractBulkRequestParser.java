/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class AbstractBulkRequestParser {

    /**
     * A parser for streamed data. Every call to {@link #parse(BytesReference, boolean)} should
     * consume as much data as possible and return the number of bytes that were consumed.
     */
    public interface IncrementalParser extends Releasable {
        /**
         * @param data the data
         * @param lastData is there more data that can be read?
         * @return the number of bytes that were parsed
         */
        int parse(BytesReference data, boolean lastData) throws IOException;
    }

    /**
     * Parse the provided {@code data} assuming the provided default values. Index requests
     * will be passed to the {@code indexRequestConsumer}, update requests to the
     * {@code updateRequestConsumer} and delete requests to the {@code deleteRequestConsumer}.
     */
    public abstract void parse(
        BytesReference data,
        @Nullable String defaultIndex,
        @Nullable String defaultRouting,
        @Nullable FetchSourceContext defaultFetchSourceContext,
        @Nullable String defaultPipeline,
        @Nullable Boolean defaultRequireAlias,
        @Nullable Boolean defaultRequireDataStream,
        @Nullable Boolean defaultListExecutedPipelines,
        boolean allowExplicitIndex,
        XContentType xContentType,
        BiConsumer<IndexRequest, String> indexRequestConsumer,
        Consumer<UpdateRequest> updateRequestConsumer,
        Consumer<DeleteRequest> deleteRequestConsumer
    ) throws IOException;

    public abstract IncrementalParser incrementalParser(
        @Nullable String defaultIndex,
        @Nullable String defaultRouting,
        @Nullable FetchSourceContext defaultFetchSourceContext,
        @Nullable String defaultPipeline,
        @Nullable Boolean defaultRequireAlias,
        @Nullable Boolean defaultRequireDataStream,
        @Nullable Boolean defaultListExecutedPipelines,
        boolean allowExplicitIndex,
        XContentType xContentType,
        BiConsumer<IndexRequest, String> indexRequestConsumer,
        Consumer<UpdateRequest> updateRequestConsumer,
        Consumer<DeleteRequest> deleteRequestConsumer
    );
}
