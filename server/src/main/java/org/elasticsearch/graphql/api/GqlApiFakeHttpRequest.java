package org.elasticsearch.graphql.api;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GqlApiFakeHttpRequest implements HttpRequest {

    private final RestRequest.Method method;
    private final String uri;
    private final BytesReference content;
    private final Map<String, List<String>> headers;

    public GqlApiFakeHttpRequest(RestRequest.Method method, String uri, BytesReference content, Map<String, List<String>> headers) {
        this.method = method;
        this.uri = uri;
        this.content = content;
        this.headers = headers;
    }

    @Override
    public RestRequest.Method method() {
        return method;
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public BytesReference content() {
        return content;
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    @Override
    public List<String> strictCookies() {
        return Collections.emptyList();
    }

    @Override
    public HttpVersion protocolVersion() {
        return HttpVersion.HTTP_1_1;
    }

    @Override
    public HttpRequest removeHeader(String header) {
        headers.remove(header);
        return this;
    }

    @Override
    public HttpResponse createResponse(RestStatus status, BytesReference content) {
        Map<String, String> headers = new HashMap<>();
        return new HttpResponse() {
            @Override
            public void addHeader(String name, String value) {
                headers.put(name, value);
            }

            @Override
            public boolean containsHeader(String name) {
                return headers.containsKey(name);
            }
        };
    }
}
