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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.bulk.byscroll.AbstractBulkByScrollRequest;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.action.bulk.byscroll.AbstractBulkByScrollRequest.SIZE_ALL_MATCHES;

/**
 * Rest handler for reindex actions that accepts a search request like Update-By-Query or Delete-By-Query
 */
public abstract class AbstractBulkByQueryRestHandler<
        Request extends AbstractBulkByScrollRequest<Request>,
        A extends GenericAction<Request, BulkByScrollResponse>> extends AbstractBaseReindexRestHandler<Request, A> {

    protected AbstractBulkByQueryRestHandler(Settings settings, A action) {
        super(settings, action);
    }

    protected void parseInternalRequest(Request internal, RestRequest restRequest,
                                        Map<String, Consumer<Object>> bodyConsumers) throws IOException {
        assert internal != null : "Request should not be null";
        assert restRequest != null : "RestRequest should not be null";

        SearchRequest searchRequest = internal.getSearchRequest();
        int scrollSize = searchRequest.source().size();
        searchRequest.source().size(SIZE_ALL_MATCHES);

        restRequest.withContentOrSourceParamParserOrNull(parser -> {
            XContentParser searchRequestParser = extractRequestSpecificFieldsAndReturnSearchCompatibleParser(parser, bodyConsumers);
            /* searchRequestParser might be parser or it might be a new parser built from parser's contents. If it is parser then
             * withContentOrSourceParamParserOrNull will close it for us but if it isn't then we should close it. Technically close on
             * the generated parser probably is a noop but we should do the accounting just in case. It doesn't hurt to close twice but it
             * really hurts not to close if by some miracle we have to. */
            try {
                RestSearchAction.parseSearchRequest(searchRequest, restRequest, searchRequestParser);
            } finally {
                IOUtils.close(searchRequestParser);
            }
        });

        internal.setSize(searchRequest.source().size());
        searchRequest.source().size(restRequest.paramAsInt("scroll_size", scrollSize));

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
    private XContentParser extractRequestSpecificFieldsAndReturnSearchCompatibleParser(XContentParser parser,
                                      Map<String, Consumer<Object>> bodyConsumers) throws IOException {
        if (parser == null) {
            return parser;
        }
        try {
            Map<String, Object> body = parser.map();

            for (Map.Entry<String, Consumer<Object>> consumer : bodyConsumers.entrySet()) {
                Object value = body.remove(consumer.getKey());
                if (value != null) {
                    consumer.getValue().accept(value);
                }
            }

            try (XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType())) {
                return parser.contentType().xContent().createParser(parser.getXContentRegistry(), builder.map(body).bytes());
            }
        } finally {
            parser.close();
        }
    }
}
