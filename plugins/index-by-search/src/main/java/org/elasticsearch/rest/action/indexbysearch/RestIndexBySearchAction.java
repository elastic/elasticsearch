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

package org.elasticsearch.rest.action.indexbysearch;

import static org.elasticsearch.action.indexbysearch.IndexBySearchAction.INSTANCE;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

import java.io.IOException;
import java.util.function.BiFunction;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.indexbysearch.IndexBySearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Expose IndexBySearchRequest over rest.
 */
public class RestIndexBySearchAction extends BaseRestHandler {
    private static final ObjectParser<IndexBySearchRequest, QueryParseContext> PARSER = new ObjectParser<>("index-by-search");
    static {
    	BiFunction<XContentParser, QueryParseContext, SearchSourceBuilder> parseSearchSource = (parser, context) -> {
    		try {
                context.reset(parser);
                return SearchSourceBuilder.parseSearchSource(parser, context);
            } catch (IOException e) {
                // TODO throw a better exception
                throw new ElasticsearchException(e);
            }
    	};
        PARSER.declareObject(IndexBySearchRequest::searchSource, parseSearchSource, new ParseField("search"));

        ObjectParser<IndexRequest, Void> indexParser = new ObjectParser<>("index");
        indexParser.declareString(IndexRequest::index, new ParseField("index"));
        indexParser.declareString(IndexRequest::type, new ParseField("type"));
        PARSER.declareField((p, v, c) -> indexParser.parse(p, v.index(), null), new ParseField("index"), ValueType.OBJECT);
    }

    private IndicesQueriesRegistry indicesQueriesRegistry;

    @Inject
    public RestIndexBySearchAction(Settings settings, RestController controller, Client client,
            IndicesQueriesRegistry indicesQueriesRegistry) {
        super(settings, controller, client);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        controller.registerHandler(POST, "/{index}/_index_by_search", this);
        controller.registerHandler(POST, "/{index}/{type}/_index_by_search", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, Client client) throws IOException {
        if (request.content() == null) {
            badRequest(channel, "body required");
            return;
        }
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));

        IndexBySearchRequest internalRequest = new IndexBySearchRequest(new SearchRequest(), new IndexRequest());
        internalRequest.search().indices(indices);
        internalRequest.search().types(types);
        internalRequest.search().indicesOptions(IndicesOptions.fromRequest(request, internalRequest.search().indicesOptions()));
        internalRequest.search().routing(request.param("routing"));

        // TODO routing for index?

        try (XContentParser xcontent = XContentFactory.xContent(request.content()).createParser(request.content())) {
            PARSER.parse(xcontent, internalRequest, new QueryParseContext(indicesQueriesRegistry));
        }


        /*
         * Fill in the index and type on the index prototype if it was not set
         * during parsing.
         */
        if (internalRequest.index().index() == null) {
            if (indices.length == 1) {
                internalRequest.index().index(indices[0]);
            } else {
                badRequest(channel, "multiple indices specified in url but index request didn't specify an index");
                return;
            }
        }
        if (internalRequest.index().type() == null) {
            if (types.length == 1) {
                internalRequest.index().type(types[0]);
            } else {
                badRequest(channel, "multiple types specified in url but index request didn't specify a type");
                return;
            }
        }

		// Fill in the query on the search if it was not set during parsing.
        if (internalRequest.search().source() == null) {
        	internalRequest.search().source(new SearchSourceBuilder());
        }
        if (internalRequest.search().source().query() == null) {
            QueryBuilder<?> queryFromUrl = RestActions.urlParamsToQueryBuilder(request);
            if (queryFromUrl != null) {
            	internalRequest.search().source().query(queryFromUrl);
            }
        }

        client.execute(INSTANCE, internalRequest, new RestToXContentListener<>(channel));
    }

    private void badRequest(RestChannel channel, String message) {
        try {
            XContentBuilder builder = channel.newErrorBuilder();
            channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder.startObject().field("error", "body required").endObject()));
        } catch (IOException e) {
            logger.warn("Failed to send response", e);
        }
    }
}
