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

package org.elasticsearch.plugin.reindex;

import static org.elasticsearch.plugin.reindex.ReindexAction.INSTANCE;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
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
import org.elasticsearch.script.Script;

/**
 * Expose IndexBySearchRequest over rest.
 */
public class RestReindexAction extends BaseRestHandler {
    private static final ObjectParser<ReindexRequest, QueryParseContext> PARSER = new ObjectParser<>("reindex");
    static {
        ObjectParser.Parser<SearchRequest, QueryParseContext> sourceParser = (parser, search, context) -> {
            try {
                /*
                 * Extract the parameters that we need from the parser. We could
                 * do away with this hack when search source has an
                 * ObjectParser.
                 */
                Map<String, Object> source = parser.map();
                Object index = source.remove("index");
                if (index != null) {
                    if (index instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<String> list = (List<String>) index;
                        search.indices(list.toArray(new String[list.size()]));
                    } else if (index instanceof String) {
                        search.indices((String) index);
                    } else {
                        throw new IllegalArgumentException("Expected index to be a list of a string but was [" + index + ']');
                    }
                }
                Object type = source.remove("type");
                if (type != null) {
                    if (type instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<String> list = (List<String>) type;
                        search.types(list.toArray(new String[list.size()]));
                    } else if (type instanceof String) {
                        search.types((String) type);
                    } else {
                        throw new IllegalArgumentException("Expected index to be a type of a string but was [" + type + ']');
                    }
                }
                XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                builder.map(source);
                parser = parser.contentType().xContent().createParser(builder.bytes());
                context.reset(parser);
                search.source().parseXContent(parser, context);
            } catch (IOException e) {
                // TODO throw a better exception
                throw new ElasticsearchException(e);
            }
        };

        ObjectParser<IndexRequest, Void> destParser = new ObjectParser<>("dest");
        destParser.declareString(IndexRequest::index, new ParseField("index"));
        destParser.declareString(IndexRequest::type, new ParseField("type"));
        destParser.declareString(IndexRequest::routing, new ParseField("routing"));
        destParser.declareString(IndexRequest::opType, new ParseField("opType"));
        destParser.declareString((s, i) -> s.versionType(VersionType.fromString(i)), new ParseField("versionType"));

        PARSER.declareField((p, v, c) -> sourceParser.parse(p, v.source(), c), new ParseField("src"), ValueType.OBJECT);
        PARSER.declareField((p, v, c) -> sourceParser.parse(p, v.source(), c), new ParseField("source"), ValueType.OBJECT);
        PARSER.declareField((p, v, c) -> destParser.parse(p, v.destination(), null), new ParseField("dest"), ValueType.OBJECT);
        PARSER.declareField((p, v, c) -> destParser.parse(p, v.destination(), null), new ParseField("destination"), ValueType.OBJECT);
        PARSER.declareInt(ReindexRequest::size, new ParseField("size"));
        PARSER.declareField((p, v, c) -> {v.script(Script.parse(p, c.parseFieldMatcher()));}, new ParseField("script"), ValueType.OBJECT);
        PARSER.declareString(ReindexRequest::conflicts, new ParseField("conflicts"));
    }

    private IndicesQueriesRegistry indicesQueriesRegistry;

    @Inject
    public RestReindexAction(Settings settings, RestController controller, Client client,
            IndicesQueriesRegistry indicesQueriesRegistry) {
        super(settings, controller, client);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        controller.registerHandler(POST, "/_reindex", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, Client client) throws IOException {
        if (request.hasContent() == false) {
            badRequest(channel, "body required");
            return;
        }

        ReindexRequest internalRequest = new ReindexRequest(new SearchRequest(), new IndexRequest());

        try (XContentParser xcontent = XContentFactory.xContent(request.content()).createParser(request.content())) {
            PARSER.parse(xcontent, internalRequest, new QueryParseContext(indicesQueriesRegistry));
        } catch (ParsingException e) {
            logger.warn("Bad request", e);
            badRequest(channel, e.getDetailedMessage());
            return;
        }
        parseCommon(internalRequest, request);

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

    public static void parseCommon(AbstractBulkByScrollRequest<?> internalRequest, RestRequest request) {
        internalRequest.refresh(request.paramAsBoolean("refresh", internalRequest.refresh()));
        internalRequest.timeout(request.paramAsTime("timeout", internalRequest.timeout()));
        String consistency = request.param("consistency");
        if (consistency != null) {
            internalRequest.consistency(WriteConsistencyLevel.fromString(consistency));
        }
    }
}
