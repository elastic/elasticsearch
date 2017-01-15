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

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;

public abstract class RestRequest implements ToXContent.Params {

    private final NamedXContentRegistry xContentRegistry;
    private final Map<String, String> params;
    private final String rawPath;
    private final Set<String> consumedParams = new HashSet<>();

    public RestRequest(NamedXContentRegistry xContentRegistry, String uri) {
        this.xContentRegistry = xContentRegistry;
        final Map<String, String> params = new HashMap<>();
        int pathEndPos = uri.indexOf('?');
        if (pathEndPos < 0) {
            this.rawPath = uri;
        } else {
            this.rawPath = uri.substring(0, pathEndPos);
            RestUtils.decodeQueryString(uri, pathEndPos + 1, params);
        }
        this.params = params;
    }

    public RestRequest(NamedXContentRegistry xContentRegistry, Map<String, String> params, String path) {
        this.xContentRegistry = xContentRegistry;
        this.params = params;
        this.rawPath = path;
    }

    public enum Method {
        GET, POST, PUT, DELETE, OPTIONS, HEAD
    }

    public abstract Method method();

    /**
     * The uri of the rest request, with the query string.
     */
    public abstract String uri();

    /**
     * The non decoded, raw path provided.
     */
    public String rawPath() {
        return rawPath;
    }

    /**
     * The path part of the URI (without the query string), decoded.
     */
    public final String path() {
        return RestUtils.decodeComponent(rawPath());
    }

    public abstract boolean hasContent();

    public abstract BytesReference content();

    public abstract String header(String name);

    public abstract Iterable<Map.Entry<String, String>> headers();

    @Nullable
    public SocketAddress getRemoteAddress() {
        return null;
    }

    @Nullable
    public SocketAddress getLocalAddress() {
        return null;
    }

    public final boolean hasParam(String key) {
        return params.containsKey(key);
    }

    @Override
    public final String param(String key) {
        consumedParams.add(key);
        return params.get(key);
    }

    @Override
    public final String param(String key, String defaultValue) {
        consumedParams.add(key);
        String value = params.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    public Map<String, String> params() {
        return params;
    }

    /**
     * Returns a list of parameters that have been consumed. This method returns a copy, callers
     * are free to modify the returned list.
     *
     * @return the list of currently consumed parameters.
     */
    List<String> consumedParams() {
        return consumedParams.stream().collect(Collectors.toList());
    }

    /**
     * Returns a list of parameters that have not yet been consumed. This method returns a copy,
     * callers are free to modify the returned list.
     *
     * @return the list of currently unconsumed parameters.
     */
    List<String> unconsumedParams() {
        return params
            .keySet()
            .stream()
            .filter(p -> !consumedParams.contains(p))
            .collect(Collectors.toList());
    }

    public float paramAsFloat(String key, float defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(sValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse float parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    public int paramAsInt(String key, int defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(sValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse int parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    public long paramAsLong(String key, long defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(sValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse long parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public boolean paramAsBoolean(String key, boolean defaultValue) {
        return Booleans.parseBoolean(param(key), defaultValue);
    }

    @Override
    public Boolean paramAsBoolean(String key, Boolean defaultValue) {
        return Booleans.parseBoolean(param(key), defaultValue);
    }

    public TimeValue paramAsTime(String key, TimeValue defaultValue) {
        return parseTimeValue(param(key), defaultValue, key);
    }

    public ByteSizeValue paramAsSize(String key, ByteSizeValue defaultValue) {
        return parseBytesSizeValue(param(key), defaultValue, key);
    }

    public String[] paramAsStringArray(String key, String[] defaultValue) {
        String value = param(key);
        if (value == null) {
            return defaultValue;
        }
        return Strings.splitStringByCommaToArray(value);
    }

    public String[] paramAsStringArrayOrEmptyIfAll(String key) {
        String[] params = paramAsStringArray(key, Strings.EMPTY_ARRAY);
        if (Strings.isAllOrWildcard(params)) {
            return Strings.EMPTY_ARRAY;
        }
        return params;
    }

    /**
     * Get the {@link NamedXContentRegistry} that should be used to create parsers from this request.
     */
    public NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }

    /**
     * A parser for the contents of this request if there is a body, otherwise throws an {@link ElasticsearchParseException}. Use
     * {@link #applyContentParser(CheckedConsumer)} if you want to gracefully handle when the request doesn't have any contents. Use
     * {@link #contentOrSourceParamParser()} for requests that support specifying the request body in the {@code source} param.
     */
    public final XContentParser contentParser() throws IOException {
        BytesReference content = content();
        if (content.length() == 0) {
            throw new ElasticsearchParseException("Body required");
        }
        return XContentFactory.xContent(content).createParser(xContentRegistry, content);
    }

    /**
     * If there is any content then call {@code applyParser} with the parser, otherwise do nothing.
     */
    public final void applyContentParser(CheckedConsumer<XContentParser, IOException> applyParser) throws IOException {
        if (hasContent()) {
            try (XContentParser parser = contentParser()) {
                applyParser.accept(parser);
            }
        }
    }

    /**
     * Does this request have content or a {@code source} parameter? Use this instead of {@link #hasContent()} if this
     * {@linkplain RestHandler} treats the {@code source} parameter like the body content.
     */
    public final boolean hasContentOrSourceParam() {
        return hasContent() || hasParam("source");
    }

    /**
     * A parser for the contents of this request if it has contents, otherwise a parser for the {@code source} parameter if there is one,
     * otherwise throws an {@link ElasticsearchParseException}. Use {@link #withContentOrSourceParamParserOrNull(CheckedConsumer)} instead
     * if you need to handle the absence request content gracefully.
     */
    public final XContentParser contentOrSourceParamParser() throws IOException {
        BytesReference content = contentOrSourceParam();
        if (content.length() == 0) {
            throw new ElasticsearchParseException("Body required");
        }
        return XContentFactory.xContent(content).createParser(xContentRegistry, content);
    }

    /**
     * Call a consumer with the parser for the contents of this request if it has contents, otherwise with a parser for the {@code source}
     * parameter if there is one, otherwise with {@code null}. Use {@link #contentOrSourceParamParser()} if you should throw an exception
     * back to the user when there isn't request content.
     */
    public final void withContentOrSourceParamParserOrNull(CheckedConsumer<XContentParser, IOException> withParser) throws IOException {
        BytesReference content = contentOrSourceParam();
        if (content.length() > 0) {
            try (XContentParser parser = XContentFactory.xContent(content).createParser(xContentRegistry, content)) {
                withParser.accept(parser);
            }
        } else {
            withParser.accept(null);
        }
    }

    /**
     * Get the content of the request or the contents of the {@code source} param. Prefer {@link #contentOrSourceParamParser()} or
     * {@link #withContentOrSourceParamParserOrNull(CheckedConsumer)} if you need a parser.
     */
    public final BytesReference contentOrSourceParam() {
        if (hasContent()) {
            return content();
        }
        String source = param("source");
        if (source != null) {
            return new BytesArray(source);
        }
        return BytesArray.EMPTY;
    }
}
