/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.AbstractBulkRequestParser;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.libs.arrow.Arrow;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ArrowBulkRequestParser extends AbstractBulkRequestParser {

    public static boolean isArrowRequest(RestRequest request) {
        return request.getParsedContentType().mediaTypeWithoutParameters().equals(Arrow.MEDIA_TYPE);
    }

    private final RestApiVersion apiVersion;
    private final DocWriteRequest.OpType defaultOpType;

    public ArrowBulkRequestParser(RestRequest request) {
        // Default operation read from the "op_type" query parameter
        // We default to create requests as it's safe and versatile:
        // - accepts requests with and without an id,
        // - if an id is present, ensures we don't accidentally overwrite an existing document,
        // - datastreams only accept create operations.
        String str = request.param("op_type", DocWriteRequest.OpType.CREATE.getLowercase());
        this.defaultOpType = DocWriteRequest.OpType.fromString(str);
        this.apiVersion = request.getRestApiVersion();
    }

    @Override
    public void parse(
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
    ) throws IOException {
        try (
            IncrementalParser parser = incrementalParser(
                defaultIndex,
                defaultRouting,
                defaultFetchSourceContext,
                defaultPipeline,
                defaultRequireAlias,
                defaultRequireDataStream,
                defaultListExecutedPipelines,
                allowExplicitIndex,
                xContentType,
                indexRequestConsumer,
                updateRequestConsumer,
                deleteRequestConsumer
            )
        ) {
            parser.parse(data, true);
        }
    }

    @Override
    public IncrementalParser incrementalParser(
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
    ) {
        return new ArrowBulkIncrementalParser(
            defaultOpType,
            defaultIndex,
            defaultRouting,
            defaultFetchSourceContext,
            defaultPipeline,
            defaultRequireAlias,
            defaultRequireDataStream,
            defaultListExecutedPipelines,
            allowExplicitIndex,
            xContentType,
            XContentParserConfiguration.EMPTY.withRestApiVersion(apiVersion),
            indexRequestConsumer,
            updateRequestConsumer,
            deleteRequestConsumer
        );
    }

}
