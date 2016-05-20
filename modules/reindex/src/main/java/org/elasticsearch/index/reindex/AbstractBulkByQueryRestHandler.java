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

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.SIZE_ALL_MATCHES;

/**
 * Rest handler for reindex actions that accepts a search request like Update-By-Query or Delete-By-Query
 */
public abstract class AbstractBulkByQueryRestHandler<
        Request extends AbstractBulkByScrollRequest<Request>,
        TA extends TransportAction<Request, BulkIndexByScrollResponse>> extends AbstractBaseReindexRestHandler<Request, TA> {

    protected AbstractBulkByQueryRestHandler(Settings settings, Client client, IndicesQueriesRegistry indicesQueriesRegistry,
                                             AggregatorParsers aggParsers, Suggesters suggesters, ClusterService clusterService,
                                             TA action) {
        super(settings, client, indicesQueriesRegistry, aggParsers, suggesters, clusterService, action);
    }

    protected void parseInternalRequest(Request internal, RestRequest restRequest,
                                        Map<String, Consumer<Object>> consumers) throws IOException {
        assert internal != null : "Request should not be null";
        assert restRequest != null : "RestRequest should not be null";

        SearchRequest searchRequest = internal.getSearchRequest();
        int scrollSize = searchRequest.source().size();
        searchRequest.source().size(SIZE_ALL_MATCHES);

        parseSearchRequest(searchRequest, restRequest, consumers);

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

    protected void parseSearchRequest(SearchRequest searchRequest, RestRequest restRequest,
                                      Map<String, Consumer<Object>> consumers) throws IOException {
        assert searchRequest != null : "SearchRequest should not be null";
        assert restRequest != null : "RestRequest should not be null";

        /*
         * We can't send parseSearchRequest REST content that it doesn't support
         * so we will have to remove the content that is valid in addition to
         * what it supports from the content first. This is a temporary hack and
         * should get better when SearchRequest has full ObjectParser support
         * then we can delegate and stuff.
         */
        BytesReference content = RestActions.hasBodyContent(restRequest) ? RestActions.getRestContent(restRequest) : null;
        if ((content != null) && (consumers != null && consumers.size() > 0)) {
            Tuple<XContentType, Map<String, Object>> body = XContentHelper.convertToMap(content, false);
            boolean modified = false;
            for (Map.Entry<String, Consumer<Object>> consumer : consumers.entrySet()) {
                Object value = body.v2().remove(consumer.getKey());
                if (value != null) {
                    consumer.getValue().accept(value);
                    modified = true;
                }
            }

            if (modified) {
                try (XContentBuilder builder = XContentFactory.contentBuilder(body.v1())) {
                    content = builder.map(body.v2()).bytes();
                }
            }
        }

        RestSearchAction.parseSearchRequest(searchRequest, indicesQueriesRegistry, restRequest, parseFieldMatcher, aggParsers,
                suggesters, content);
    }
}
