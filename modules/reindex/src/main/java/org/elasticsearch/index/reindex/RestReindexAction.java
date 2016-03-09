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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

/**
 * Expose IndexBySearchRequest over rest.
 */
public class RestReindexAction extends AbstractBaseReindexRestHandler<ReindexRequest, ReindexResponse, TransportReindexAction> {
    @Inject
    public RestReindexAction(Settings settings, RestController controller, Client client, ClusterService clusterService,
            IndicesQueriesRegistry indicesQueriesRegistry, AggregatorParsers aggParsers, Suggesters suggesters,
            TransportReindexAction action) {
        super(settings, controller, client, clusterService, indicesQueriesRegistry, aggParsers, suggesters, action);
        controller.registerHandler(POST, "/_reindex", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, Client client) throws IOException {
        if (false == request.hasContent()) {
            badRequest(channel, "body required");
            return;
        }

        ReindexRequest internalRequest = new ReindexRequest(new SearchRequest(), new IndexRequest());

        try (XContentParser xcontent = XContentFactory.xContent(request.content()).createParser(request.content())) {
            parseRequest(xcontent, internalRequest);
        } catch (ElasticsearchParseException e) {
            logger.warn("Bad request", e);
            badRequest(channel, e.getDetailedMessage());
            return;
        }
        parseCommon(internalRequest, request);

        execute(request, internalRequest, channel);
    }

    private void parseRequest(XContentParser parser, ReindexRequest request) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("Reindex's request body must be an object.");
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                switch (currentFieldName) {
                case "source":
                    parseSource(parser, request.getSearchRequest());
                    break;
                case "dest":
                    parseDest(parser, request.getDestination());
                    break;
                case "script":
                    request.setScript(Script.parse(parser, parseFieldMatcher));
                    break;
                default:
                    throw new ElasticsearchParseException("Unknown object field [{}]", currentFieldName);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                throw new ElasticsearchParseException("Unknown array field [{}]", currentFieldName);
            } else if (token.isValue()) {
                switch (currentFieldName) {
                case "size":
                    request.setSize(parser.intValue());
                    break;
                case "conflicts":
                    request.setConflicts(parser.text());
                    break;
                default:
                    throw new ElasticsearchParseException("Unknown value field [{}]", currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("Unexpected token type [{}]", token);
            }
        }
    }
    
    private void parseSource(XContentParser parser, SearchRequest search) throws IOException {
        /*
         * Extract the parameters that we need from the parser. We could do
         * away with this hack when search source has an ObjectParser.
         */
        Map<String, Object> source = parser.map();
        String[] indices = extractStringArray(source, "index");
        if (indices != null) {
            search.indices(indices);
        }
        String[] types = extractStringArray(source, "type");
        if (types != null) {
            search.types(types);
        }
        XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
        builder.map(source);
        search.extraSource(builder);
    }

    private void parseDest(XContentParser parser, IndexRequest index) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("Unknown object field [{}]", currentFieldName);
            } else if (token == XContentParser.Token.START_ARRAY) {
                throw new ElasticsearchParseException("Unknown array field [{}]", currentFieldName);
            } else if (token.isValue()) {
                switch (currentFieldName) {
                case "index":
                    index.index(parser.text());
                    break;
                case "type":
                    index.type(parser.text());
                    break;
                case "routing":
                    index.routing(parser.text());
                    break;
                case "opType":
                case "op_type":
                    index.opType(parser.text());
                    break;
                case "versionType":
                case "version_type":
                    index.versionType(VersionType.fromString(parser.text()));
                    break;
                case "timestamp": // This isn't actually supported but the validator will catch it and make a nice error message
                    index.timestamp(parser.text());
                    break;
                case "ttl": // This isn't actually supported but the validator will catch it and make a nice error message
                    index.ttl(parser.text());
                    break;
                default:
                    throw new ElasticsearchParseException("Unknown value field [{}]", currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("Unexpected token type [{}]", token);
            }
        }
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
        internalRequest.setRefresh(request.paramAsBoolean("refresh", internalRequest.isRefresh()));
        internalRequest.setTimeout(request.paramAsTime("timeout", internalRequest.getTimeout()));
        String consistency = request.param("consistency");
        if (consistency != null) {
            internalRequest.setConsistency(WriteConsistencyLevel.fromString(consistency));
        }
    }

    /**
     * Yank a string array from a map. Emulates XContent's permissive String to
     * String array conversions.
     */
    private static String[] extractStringArray(Map<String, Object> source, String name) {
        Object value = source.remove(name);
        if (value == null) {
            return null;
        }
        if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> list = (List<String>) value;
            return list.toArray(new String[list.size()]);
        } else if (value instanceof String) {
            return new String[] {(String) value};
        } else {
            throw new IllegalArgumentException("Expected [" + name + "] to be a list of a string but was [" + value + ']');
        }
    }
}