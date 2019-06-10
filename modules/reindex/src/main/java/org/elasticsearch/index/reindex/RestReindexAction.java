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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Expose reindex over rest.
 */
public class RestReindexAction extends AbstractBaseReindexRestHandler<ReindexRequest, ReindexAction> {
    static final ObjectParser<ReindexRequest, Void> PARSER = new ObjectParser<>("reindex");
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in reindex requests is deprecated.";
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestReindexAction.class));

    static {
        ObjectParser.Parser<ReindexRequest, Void> sourceParser = (parser, request, context) -> {
            // Funky hack to work around Search not having a proper ObjectParser and us wanting to extract query if using remote.
            Map<String, Object> source = parser.map();
            String[] indices = extractStringArray(source, "index");
            if (indices != null) {
                request.getSearchRequest().indices(indices);
            }
            request.setRemoteInfo(buildRemoteInfo(source));
            XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
            builder.map(source);
            try (InputStream stream = BytesReference.bytes(builder).streamInput();
                 XContentParser innerParser = parser.contentType().xContent()
                     .createParser(parser.getXContentRegistry(), parser.getDeprecationHandler(), stream)) {
                request.getSearchRequest().source().parseXContent(innerParser, false);
            }
        };

        ObjectParser<IndexRequest, Void> destParser = new ObjectParser<>("dest");
        destParser.declareString(IndexRequest::index, new ParseField("index"));
        destParser.declareString((request, type) -> {
            deprecationLogger.deprecatedAndMaybeLog("reindex_with_types", TYPES_DEPRECATION_MESSAGE);
            request.type(type);
        }, new ParseField("type"));
        destParser.declareString(IndexRequest::routing, new ParseField("routing"));
        destParser.declareString(IndexRequest::opType, new ParseField("op_type"));
        destParser.declareString(IndexRequest::setPipeline, new ParseField("pipeline"));
        destParser.declareString((s, i) -> s.versionType(VersionType.fromString(i)), new ParseField("version_type"));

        PARSER.declareField(sourceParser::parse, new ParseField("source"), ValueType.OBJECT);
        PARSER.declareField((p, v, c) -> destParser.parse(p, v.getDestination(), c), new ParseField("dest"), ValueType.OBJECT);
        PARSER.declareInt(RestReindexAction::setMaxDocsValidateIdentical, new ParseField("max_docs", "size"));
        PARSER.declareField((p, v, c) -> v.setScript(Script.parse(p)), new ParseField("script"),
                ValueType.OBJECT);
        PARSER.declareString(ReindexRequest::setConflicts, new ParseField("conflicts"));
    }

    public RestReindexAction(Settings settings, RestController controller) {
        super(settings, ReindexAction.INSTANCE);
        controller.registerHandler(POST, "/_reindex", this);
    }

    @Override
    public String getName() {
        return "reindex_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return doPrepareRequest(request, client, true, true);
    }

    @Override
    protected ReindexRequest buildRequest(RestRequest request) throws IOException {
        if (request.hasParam("pipeline")) {
            throw new IllegalArgumentException("_reindex doesn't support [pipeline] as a query parameter. "
                    + "Specify it in the [dest] object instead.");
        }
        ReindexRequest internal = new ReindexRequest();
        try (XContentParser parser = request.contentParser()) {
            PARSER.parse(parser, internal, null);
        }
        if (request.hasParam("scroll")) {
            internal.setScroll(parseTimeValue(request.param("scroll"), "scroll"));
        }
        return internal;
    }

    static RemoteInfo buildRemoteInfo(Map<String, Object> source) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> remote = (Map<String, Object>) source.remove("remote");
        if (remote == null) {
            return null;
        }
        String username = extractString(remote, "username");
        String password = extractString(remote, "password");
        String hostInRequest = requireNonNull(extractString(remote, "host"), "[host] must be specified to reindex from a remote cluster");
        URI uri;
        try {
            uri = new URI(hostInRequest);
            // URI has less stringent URL parsing than our code. We want to fail if all values are not provided.
            if (uri.getPort() == -1) {
                throw new URISyntaxException(hostInRequest, "The port was not defined in the [host]");
            }
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("[host] must be of the form [scheme]://[host]:[port](/[pathPrefix])? but was ["
                + hostInRequest + "]", ex);
        }

        String scheme = uri.getScheme();
        String host = uri.getHost();
        int port = uri.getPort();

        String pathPrefix = null;
        if (uri.getPath().isEmpty() == false) {
            pathPrefix = uri.getPath();
        }

        Map<String, String> headers = extractStringStringMap(remote, "headers");
        TimeValue socketTimeout = extractTimeValue(remote, "socket_timeout", RemoteInfo.DEFAULT_SOCKET_TIMEOUT);
        TimeValue connectTimeout = extractTimeValue(remote, "connect_timeout", RemoteInfo.DEFAULT_CONNECT_TIMEOUT);
        if (false == remote.isEmpty()) {
            throw new IllegalArgumentException(
                    "Unsupported fields in [remote]: [" + Strings.collectionToCommaDelimitedString(remote.keySet()) + "]");
        }
        return new RemoteInfo(scheme, host, port, pathPrefix, queryForRemote(source),
            username, password, headers, socketTimeout, connectTimeout);
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

    private static String extractString(Map<String, Object> source, String name) {
        Object value = source.remove(name);
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        throw new IllegalArgumentException("Expected [" + name + "] to be a string but was [" + value + "]");
    }

    private static Map<String, String> extractStringStringMap(Map<String, Object> source, String name) {
        Object value = source.remove(name);
        if (value == null) {
            return emptyMap();
        }
        if (false == value instanceof Map) {
            throw new IllegalArgumentException("Expected [" + name + "] to be an object containing strings but was [" + value + "]");
        }
        Map<?, ?> map = (Map<?, ?>) value;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (false == entry.getKey() instanceof String || false == entry.getValue() instanceof String) {
                throw new IllegalArgumentException("Expected [" + name + "] to be an object containing strings but has [" + entry + "]");
            }
        }
        @SuppressWarnings("unchecked") // We just checked....
        Map<String, String> safe = (Map<String, String>) map;
        return safe;
    }

    private static TimeValue extractTimeValue(Map<String, Object> source, String name, TimeValue defaultValue) {
        String string = extractString(source, name);
        return string == null ? defaultValue : parseTimeValue(string, name);
    }

    private static BytesReference queryForRemote(Map<String, Object> source) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        Object query = source.remove("query");
        if (query == null) {
            return BytesReference.bytes(matchAllQuery().toXContent(builder, ToXContent.EMPTY_PARAMS));
        }
        if (!(query instanceof Map)) {
            throw new IllegalArgumentException("Expected [query] to be an object but was [" + query + "]");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) query;
        return BytesReference.bytes(builder.map(map));
    }
}
