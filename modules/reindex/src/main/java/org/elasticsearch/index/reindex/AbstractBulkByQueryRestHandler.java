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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Rest handler for reindex actions that accepts a search request like Update-By-Query or Delete-By-Query
 */
public abstract class AbstractBulkByQueryRestHandler<
        Request extends AbstractBulkByScrollRequest<Request>,
        A extends ActionType<BulkByScrollResponse>> extends AbstractBaseReindexRestHandler<Request, A> {

    protected AbstractBulkByQueryRestHandler(A action) {
        super(action);
    }

    protected void parseInternalRequest(Request internal, RestRequest restRequest,
                                        Map<String, Consumer<Object>> bodyConsumers) throws IOException {
        assert internal != null : "Request should not be null";
        assert restRequest != null : "RestRequest should not be null";

        SearchRequest searchRequest = internal.getSearchRequest();

        try (XContentParser parser = extractRequestSpecificFields(restRequest, bodyConsumers)) {
            RestSearchAction.parseSearchRequest(searchRequest, restRequest, parser, size -> failOnSizeSpecified());
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
}
