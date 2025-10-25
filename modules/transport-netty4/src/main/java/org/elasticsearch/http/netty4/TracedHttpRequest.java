package org.elasticsearch.http.netty4;

import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.telemetry.tracing.Traceable;

public class TracedHttpRequest implements Traceable, HttpRequest {

    private HttpRequest delegate;
    private String spanId;

    public TracedHttpRequest(HttpRequest request) {
        this.delegate = request;
        this.spanId = UUIDs.randomBase64UUID(Randomness.get());
    }

    @Override
    public HttpMethod getMethod() {
        return delegate.getMethod();
    }

    @Override
    public HttpMethod method() {
        return delegate.method();
    }

    @Override
    public HttpRequest setMethod(HttpMethod method) {
        return delegate.setMethod(method);
    }

    @Override
    public String getUri() {
        return delegate.getUri();
    }

    @Override
    public String uri() {
        return delegate.uri();
    }

    @Override
    public HttpRequest setUri(String uri) {
        return delegate.setUri(uri);
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return delegate.getProtocolVersion();
    }

    @Override
    public HttpVersion protocolVersion() {
        return delegate.protocolVersion();
    }

    @Override
    public HttpRequest setProtocolVersion(HttpVersion version) {
        return delegate.setProtocolVersion(version);
    }

    @Override
    public HttpHeaders headers() {
        return delegate.headers();
    }

    @Override
    public DecoderResult getDecoderResult() {
        return delegate.getDecoderResult();
    }

    @Override
    public DecoderResult decoderResult() {
        return delegate.decoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
        delegate.setDecoderResult(result);
    }

    @Override
    public String getSpanId() {
        return spanId;
    }
}
