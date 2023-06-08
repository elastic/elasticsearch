/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.xcontent.ParsedMediaType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;
import static org.elasticsearch.core.TimeValue.parseTimeValue;

public class RestRequest implements ToXContent.Params {

    public static final String RESPONSE_RESTRICTED = "responseRestricted";
    // tchar pattern as defined by RFC7230 section 3.2.6
    private static final Pattern TCHAR_PATTERN = Pattern.compile("[a-zA-Z0-9!#$%&'*+\\-.\\^_`|~]+");

    private static final AtomicLong requestIdGenerator = new AtomicLong();

    private final XContentParserConfiguration parserConfig;
    private final Map<String, String> params;
    private final Map<String, List<String>> headers;
    private final String rawPath;
    private final Set<String> consumedParams = new HashSet<>();
    private final SetOnce<XContentType> xContentType = new SetOnce<>();
    private final HttpChannel httpChannel;
    private final ParsedMediaType parsedAccept;
    private final ParsedMediaType parsedContentType;
    private final RestApiVersion restApiVersion;
    private HttpRequest httpRequest;

    private boolean contentConsumed = false;

    private final long requestId;

    public boolean isContentConsumed() {
        return contentConsumed;
    }

    protected RestRequest(
        XContentParserConfiguration parserConfig,
        Map<String, String> params,
        String path,
        Map<String, List<String>> headers,
        HttpRequest httpRequest,
        HttpChannel httpChannel
    ) {
        this(parserConfig, params, path, headers, httpRequest, httpChannel, requestIdGenerator.incrementAndGet());
    }

    private RestRequest(
        XContentParserConfiguration parserConfig,
        Map<String, String> params,
        String path,
        Map<String, List<String>> headers,
        HttpRequest httpRequest,
        HttpChannel httpChannel,
        long requestId
    ) {
        try {
            this.parsedAccept = parseHeaderWithMediaType(httpRequest.getHeaders(), "Accept");
        } catch (IllegalArgumentException e) {
            throw new MediaTypeHeaderException(e, "Accept");
        }
        try {
            this.parsedContentType = parseHeaderWithMediaType(httpRequest.getHeaders(), "Content-Type");
            if (parsedContentType != null) {
                this.xContentType.set(parsedContentType.toMediaType(XContentType.MEDIA_TYPE_REGISTRY));
            }
        } catch (IllegalArgumentException e) {
            throw new MediaTypeHeaderException(e, "Content-Type");
        }
        this.httpRequest = httpRequest;
        try {
            this.restApiVersion = RestCompatibleVersionHelper.getCompatibleVersion(parsedAccept, parsedContentType, hasContent());
        } catch (ElasticsearchStatusException e) {
            throw new MediaTypeHeaderException(e, "Accept", "Content-Type");
        }
        this.parserConfig = parserConfig.restApiVersion().equals(restApiVersion)
            ? parserConfig
            : parserConfig.withRestApiVersion(restApiVersion);
        this.httpChannel = httpChannel;
        this.params = params;
        this.rawPath = path;
        this.headers = Collections.unmodifiableMap(headers);
        this.requestId = requestId;
    }

    protected RestRequest(RestRequest other) {
        assert other.parserConfig.restApiVersion().equals(other.restApiVersion);
        this.parsedAccept = other.parsedAccept;
        this.parsedContentType = other.parsedContentType;
        if (other.xContentType.get() != null) {
            this.xContentType.set(other.xContentType.get());
        }
        this.restApiVersion = other.restApiVersion;
        this.parserConfig = other.parserConfig;
        this.httpRequest = other.httpRequest;
        this.httpChannel = other.httpChannel;
        this.params = other.params;
        this.rawPath = other.rawPath;
        this.headers = other.headers;
        this.requestId = other.requestId;
    }

    private static @Nullable ParsedMediaType parseHeaderWithMediaType(Map<String, List<String>> headers, String headerName) {
        // TODO: make all usages of headers case-insensitive
        List<String> header = headers.get(headerName);
        if (header == null || header.isEmpty()) {
            return null;
        } else if (header.size() > 1) {
            throw new IllegalArgumentException("Incorrect header [" + headerName + "]. Only one value should be provided");
        }
        String rawContentType = header.get(0);
        if (Strings.hasText(rawContentType)) {
            return ParsedMediaType.parseMediaType(rawContentType);
        } else {
            throw new IllegalArgumentException("Header [" + headerName + "] cannot be empty.");
        }
    }

    /**
     * Invoke {@link HttpRequest#releaseAndCopy()} on the http request in this instance and replace a pooled http request
     * with an unpooled copy. This is supposed to be used before passing requests to {@link RestHandler} instances that can not safely
     * handle http requests that use pooled buffers as determined by {@link RestHandler#allowsUnsafeBuffers()}.
     */
    void ensureSafeBuffers() {
        httpRequest = httpRequest.releaseAndCopy();
    }

    /**
     * Creates a new REST request.
     *
     * @throws BadParameterException if the parameters can not be decoded
     * @throws MediaTypeHeaderException if the Content-Type or Accept header can not be parsed
     */
    public static RestRequest request(XContentParserConfiguration parserConfig, HttpRequest httpRequest, HttpChannel httpChannel) {
        Map<String, String> params = params(httpRequest.uri());
        String path = path(httpRequest.uri());
        return new RestRequest(
            parserConfig,
            params,
            path,
            httpRequest.getHeaders(),
            httpRequest,
            httpChannel,
            requestIdGenerator.incrementAndGet()
        );
    }

    private static Map<String, String> params(final String uri) {
        final Map<String, String> params = new HashMap<>();
        int index = uri.indexOf('?');
        if (index >= 0) {
            try {
                RestUtils.decodeQueryString(uri, index + 1, params);
            } catch (final IllegalArgumentException e) {
                throw new BadParameterException(e);
            }
        }
        return params;
    }

    private static String path(final String uri) {
        final int index = uri.indexOf('?');
        if (index >= 0) {
            return uri.substring(0, index);
        } else {
            return uri;
        }
    }

    /**
     * Creates a new REST request. The path is not decoded so this constructor will not throw a
     * {@link BadParameterException}.
     *
     * @throws MediaTypeHeaderException if the Content-Type or Accept header can not be parsed
     */
    public static RestRequest requestWithoutParameters(
        XContentParserConfiguration parserConfig,
        HttpRequest httpRequest,
        HttpChannel httpChannel
    ) {
        Map<String, String> params = Collections.emptyMap();
        return new RestRequest(
            parserConfig,
            params,
            httpRequest.uri(),
            httpRequest.getHeaders(),
            httpRequest,
            httpChannel,
            requestIdGenerator.incrementAndGet()
        );
    }

    public enum Method {
        GET,
        POST,
        PUT,
        DELETE,
        OPTIONS,
        HEAD,
        PATCH,
        TRACE,
        CONNECT
    }

    /**
     * Returns the HTTP method used in the REST request.
     *
     * @return the {@link Method} used in the REST request
     * @throws IllegalArgumentException if the HTTP method is invalid
     */
    public Method method() {
        return httpRequest.method();
    }

    /**
     * The uri of the rest request, with the query string.
     */
    public String uri() {
        return httpRequest.uri();
    }

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

    public boolean hasContent() {
        return contentLength() > 0;
    }

    public int contentLength() {
        return httpRequest.content().length();
    }

    public BytesReference content() {
        this.contentConsumed = true;
        return httpRequest.content();
    }

    /**
     * @return content of the request body or throw an exception if the body or content type is missing
     */
    public final BytesReference requiredContent() {
        if (hasContent() == false) {
            throw new ElasticsearchParseException("request body is required");
        } else if (xContentType.get() == null) {
            throw new IllegalStateException("unknown content type");
        }
        return content();
    }

    /**
     * Get the value of the header or {@code null} if not found. This method only retrieves the first header value if multiple values are
     * sent. Use of {@link #getAllHeaderValues(String)} should be preferred
     */
    public final String header(String name) {
        List<String> values = headers.get(name);
        if (values != null && values.isEmpty() == false) {
            return values.get(0);
        }
        return null;
    }

    /**
     * Get all values for the header or {@code null} if the header was not found
     */
    public final List<String> getAllHeaderValues(String name) {
        List<String> values = headers.get(name);
        if (values != null) {
            return Collections.unmodifiableList(values);
        }
        return null;
    }

    /**
     * Get all of the headers and values associated with the headers. Modifications of this map are not supported.
     */
    public final Map<String, List<String>> getHeaders() {
        return headers;
    }

    public final long getRequestId() {
        return requestId;
    }

    /**
     * The {@link XContentType} that was parsed from the {@code Content-Type} header. This value will be {@code null} in the case of
     * a request without a valid {@code Content-Type} header, a request without content ({@link #hasContent()}, or a plain text request
     */
    @Nullable
    public final XContentType getXContentType() {
        return xContentType.get();
    }

    public HttpChannel getHttpChannel() {
        return httpChannel;
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
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
        return new ArrayList<>(consumedParams);
    }

    /**
     * Returns a list of parameters that have not yet been consumed. This method returns a copy,
     * callers are free to modify the returned list.
     *
     * @return the list of currently unconsumed parameters.
     */
    List<String> unconsumedParams() {
        return params.keySet().stream().filter(p -> consumedParams.contains(p) == false).toList();
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

    public double paramAsDouble(String key, double defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(sValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse double parameter [" + key + "] with value [" + sValue + "]", e);
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
        String rawParam = param(key);
        // Treat empty string as true because that allows the presence of the url parameter to mean "turn this on"
        if (rawParam != null && rawParam.length() == 0) {
            return true;
        } else {
            return Booleans.parseBoolean(rawParam, defaultValue);
        }
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
     * Get the configuration that should be used to create {@link XContentParser} from this request.
     */
    public XContentParserConfiguration contentParserConfig() {
        return parserConfig;
    }

    /**
     * A parser for the contents of this request if there is a body, otherwise throws an {@link ElasticsearchParseException}. Use
     * {@link #applyContentParser(CheckedConsumer)} if you want to gracefully handle when the request doesn't have any contents. Use
     * {@link #contentOrSourceParamParser()} for requests that support specifying the request body in the {@code source} param.
     */
    public final XContentParser contentParser() throws IOException {
        BytesReference content = requiredContent(); // will throw exception if body or content type missing
        XContent xContent = xContentType.get().xContent();
        return xContent.createParser(parserConfig, content.streamInput());

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
        Tuple<XContentType, BytesReference> tuple = contentOrSourceParam();
        return tuple.v1().xContent().createParser(parserConfig, tuple.v2().streamInput());
    }

    /**
     * Call a consumer with the parser for the contents of this request if it has contents, otherwise with a parser for the {@code source}
     * parameter if there is one, otherwise with {@code null}. Use {@link #contentOrSourceParamParser()} if you should throw an exception
     * back to the user when there isn't request content.
     */
    public final void withContentOrSourceParamParserOrNull(CheckedConsumer<XContentParser, IOException> withParser) throws IOException {
        if (hasContentOrSourceParam()) {
            Tuple<XContentType, BytesReference> tuple = contentOrSourceParam();
            BytesReference content = tuple.v2();
            XContentType xContentType = tuple.v1();
            try (
                InputStream stream = content.streamInput();
                XContentParser parser = xContentType.xContent().createParser(parserConfig, stream)
            ) {
                withParser.accept(parser);
            }
        } else {
            withParser.accept(null);
        }
    }

    /**
     * Get the content of the request or the contents of the {@code source} param or throw an exception if both are missing.
     * Prefer {@link #contentOrSourceParamParser()} or {@link #withContentOrSourceParamParserOrNull(CheckedConsumer)} if you need a parser.
     */
    public final Tuple<XContentType, BytesReference> contentOrSourceParam() {
        if (hasContentOrSourceParam() == false) {
            throw new ElasticsearchParseException("request body or source parameter is required");
        } else if (hasContent()) {
            return new Tuple<>(xContentType.get(), requiredContent());
        }
        String source = param("source");
        String typeParam = param("source_content_type");
        if (source == null || typeParam == null) {
            throw new IllegalStateException("source and source_content_type parameters are required");
        }
        BytesArray bytes = new BytesArray(source);
        final XContentType xContentType = parseContentType(Collections.singletonList(typeParam));
        if (xContentType == null) {
            throw new IllegalStateException("Unknown value for source_content_type [" + typeParam + "]");
        }
        return new Tuple<>(xContentType, bytes);
    }

    public ParsedMediaType getParsedAccept() {
        return parsedAccept;
    }

    public ParsedMediaType getParsedContentType() {
        return parsedContentType;
    }

    /**
     * Parses the given content type string for the media type. This method currently ignores parameters.
     */
    // TODO stop ignoring parameters such as charset...
    public static XContentType parseContentType(List<String> header) {
        if (header == null || header.isEmpty()) {
            return null;
        } else if (header.size() > 1) {
            throw new IllegalArgumentException("only one Content-Type header should be provided");
        }

        String rawContentType = header.get(0);
        final String[] elements = rawContentType.split("[ \t]*;");
        if (elements.length > 0) {
            final String[] splitMediaType = elements[0].split("/");
            if (splitMediaType.length == 2
                && TCHAR_PATTERN.matcher(splitMediaType[0]).matches()
                && TCHAR_PATTERN.matcher(splitMediaType[1].trim()).matches()) {
                return XContentType.fromMediaType(elements[0]);
            } else {
                throw new IllegalArgumentException("invalid Content-Type header [" + rawContentType + "]");
            }
        }
        throw new IllegalArgumentException("empty Content-Type header");
    }

    /**
     * The requested version of the REST API.
     */
    public RestApiVersion getRestApiVersion() {
        return restApiVersion;
    }

    public void markResponseRestricted(String restriction) {
        params.put(RESPONSE_RESTRICTED, restriction);
    }

    public static class MediaTypeHeaderException extends RuntimeException {

        private final String message;
        private final Set<String> failedHeaderNames;

        MediaTypeHeaderException(final RuntimeException cause, String... failedHeaderNames) {
            super(cause);
            this.failedHeaderNames = Set.of(failedHeaderNames);
            this.message = "Invalid media-type value on headers " + this.failedHeaderNames;
        }

        public Set<String> getFailedHeaderNames() {
            return failedHeaderNames;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    public static class BadParameterException extends RuntimeException {

        BadParameterException(final IllegalArgumentException cause) {
            super(cause);
        }

    }

}
