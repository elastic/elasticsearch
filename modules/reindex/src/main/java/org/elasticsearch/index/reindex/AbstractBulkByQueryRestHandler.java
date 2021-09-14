/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Rest handler for reindex actions that accepts a search request like Update-By-Query or Delete-By-Query
 */
public abstract class AbstractBulkByQueryRestHandler<
        Request extends AbstractBulkByScrollRequest<Request>,
        A extends ActionType<BulkByScrollResponse>> extends AbstractBaseReindexRestHandler<Request, A> {

    protected AbstractBulkByQueryRestHandler(A action) {
        super(action);
    }

    protected void parseInternalRequest(Request internal, RestRequest restRequest, NamedWriteableRegistry namedWriteableRegistry,
                                        Map<String, Consumer<Object>> bodyConsumers) throws IOException {
        assert internal != null : "Request should not be null";
        assert restRequest != null : "RestRequest should not be null";

        SearchRequest searchRequest = internal.getSearchRequest();

        try (XContentParser parser = extractRequestSpecificFields(restRequest, bodyConsumers)) {
            IntConsumer sizeConsumer = restRequest.getRestApiVersion() == RestApiVersion.V_7 ?
                size -> setMaxDocsFromSearchSize(internal, size) :
                size -> failOnSizeSpecified();
            RestSearchAction.parseSearchRequest(searchRequest, restRequest, parser, namedWriteableRegistry, sizeConsumer);
        }

        searchRequest.source().size(restRequest.paramAsInt("scroll_size", searchRequest.source().size()));

        String conflicts = restRequest.param("conflicts");
        if (conflicts != null) {
            internal.setConflicts(conflicts);
        }

        // Let the requester set search timeout. It is probably only going to be useful for testing but who knows.
        if (restRequest.hasParam("search_timeout")) {
            searchRequest.source().timeout(restRequest.paramAsTime("search_timeout", null));
        }
    }

    /**
     * We can't send parseSearchRequest REST content that it doesn't support
     * so we will have to remove the content that is valid in addition to
     * what it supports from the content first. This is a temporary hack and
     * should get better when SearchRequest has full ObjectParser support
     * then we can delegate and stuff.
     */
    private XContentParser extractRequestSpecificFields(RestRequest restRequest,
                                                        Map<String, Consumer<Object>> bodyConsumers) throws IOException {
        if (restRequest.hasContentOrSourceParam() == false) {
            return null; // body is optional
        }
        try (XContentParser parser = restRequest.contentOrSourceParamParser();
             XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType())) {
            Map<String, Object> body = parser.map();

            for (Map.Entry<String, Consumer<Object>> consumer : bodyConsumers.entrySet()) {
                Object value = body.remove(consumer.getKey());
                if (value != null) {
                    consumer.getValue().accept(value);
                }
            }
            return parser.contentType().xContent().createParser(parser.getXContentRegistry(),
                parser.getDeprecationHandler(), BytesReference.bytes(builder.map(body)).streamInput());
        }
    }

    private static void failOnSizeSpecified() {
        throw new IllegalArgumentException("invalid parameter [size], use [max_docs] instead");
    }

    private void setMaxDocsFromSearchSize(Request request, int size) {
        LoggingDeprecationHandler.INSTANCE.logRenamedField(null, null, "size", "max_docs", true);
        setMaxDocsValidateIdentical(request, size);
    }
}
