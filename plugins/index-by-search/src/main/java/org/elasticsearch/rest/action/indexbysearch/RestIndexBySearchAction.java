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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.indexbysearch.IndexBySearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestToXContentListener;

/**
 * Expose IndexBySearchRequest over rest.
 */
public class RestIndexBySearchAction extends BaseRestHandler {
    private static final ObjectParser<IndexBySearchRequest, QueryParseContext> PARSER = new ObjectParser<>("index-by-search");
    static {
        ObjectParser<SearchRequest, QueryParseContext> sourceParser = new ObjectParser<>("source");
        sourceParser.declareStringArray((s, i) -> s.indices(i.toArray(new String[i.size()])), new ParseField("index"));
        sourceParser.declareStringArray((s, i) -> s.types(i.toArray(new String[i.size()])), new ParseField("type"));
        ObjectParser.Parser<SearchRequest, QueryParseContext> parseSearchSource = (parser, search, context) -> {
            try {
                context.reset(parser);
                search.source().parseXConent(parser, context);
            } catch (IOException e) {
                // TODO throw a better exception
                throw new ElasticsearchException(e);
            }
        };
        sourceParser.declareField(parseSearchSource, new ParseField("search"), ValueType.OBJECT);

        ObjectParser<IndexRequest, Void> indexParser = new ObjectParser<>("index");
        indexParser.declareString(IndexRequest::index, new ParseField("index"));
        indexParser.declareString(IndexRequest::type, new ParseField("type"));
        indexParser.declareString((i, v) -> i.versionType(VersionType.fromString(v)), new ParseField("version_type"));
        indexParser.declareString(IndexRequest::routing, new ParseField("routing"));

        PARSER.declareField((p, v, c) -> sourceParser.parse(p, v.search(), c), new ParseField("source"), ValueType.OBJECT);
        PARSER.declareField((p, v, c) -> indexParser.parse(p, v.index(), null), new ParseField("index"), ValueType.OBJECT);
        PARSER.declareInt(IndexBySearchRequest::size, new ParseField("size"));
    }

    private IndicesQueriesRegistry indicesQueriesRegistry;

    @Inject
    public RestIndexBySearchAction(Settings settings, RestController controller, Client client,
            IndicesQueriesRegistry indicesQueriesRegistry) {
        super(settings, controller, client);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        controller.registerHandler(POST, "/_index_by_search", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, Client client) throws IOException {
        if (request.hasContent() == false) {
            badRequest(channel, "body required");
            return;
        }

        IndexBySearchRequest internalRequest = new IndexBySearchRequest(new SearchRequest(), new IndexRequest());

        try (XContentParser xcontent = XContentFactory.xContent(request.content()).createParser(request.content())) {
            PARSER.parse(xcontent, internalRequest, new QueryParseContext(indicesQueriesRegistry));
        } catch (ParsingException e) {
            logger.warn("Bad request", e);
            badRequest(channel, e.getDetailedMessage());
            return;
        }

        client.execute(INSTANCE, internalRequest, new RestToXContentListener<>(channel));
    }

    private void badRequest(RestChannel channel, String message) {
        try {
            XContentBuilder builder = channel.newErrorBuilder();
            channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder.startObject().field("error", message).endObject()));
        } catch (IOException e) {
            logger.warn("Failed to send response", e);
        }
    }
}
