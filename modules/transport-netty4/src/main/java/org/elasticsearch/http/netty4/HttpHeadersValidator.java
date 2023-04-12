/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.netty4.HttpHeadersValidator.ValidatableHttpHeaders.ValidationContext;
import org.elasticsearch.rest.RestRequest;

import java.util.List;
import java.util.Map;

public final class HttpHeadersValidator {

    public static HttpHeadersValidator NOOP_VALIDATOR = new HttpHeadersValidator(
        (httpPreRequest, channel, listener) -> listener.onResponse(null)
    );

    private final TriConsumer<HttpPreRequest, Channel, ActionListener<ValidationContext>> validator;

    public HttpHeadersValidator(TriConsumer<HttpPreRequest, Channel, ActionListener<ValidationContext>> validator) {
        this.validator = validator;
    }

    public Netty4HttpHeaderValidator getValidatorInboundHandler() {
        return new Netty4HttpHeaderValidator((httpRequest, channel, listener) -> {
            if ((httpRequest.headers() instanceof ValidatableHttpHeaders) == false) {
                listener.onFailure(new IllegalStateException("Expected a validatable request for validation"));
                return;
            }
            ValidatableHttpHeaders validatableHttpHeaders = ((ValidatableHttpHeaders) httpRequest.headers());
            validator.apply(asHttpPreRequest(httpRequest), channel, listener.delegateFailure((l, response) -> {
                validatableHttpHeaders.markValidationSucceeded(response);
                l.onResponse(null);
            }));
        });
    }

    public HttpMessage wrapAsValidatableMessage(HttpMessage newlyDecodedMessage) {
        DefaultHttpRequest httpRequest = (DefaultHttpRequest) newlyDecodedMessage;
        ValidatableHttpHeaders validatableHttpHeaders = new ValidatableHttpHeaders(newlyDecodedMessage.headers());
        return new DefaultHttpRequest(httpRequest.protocolVersion(), httpRequest.method(), httpRequest.uri(), validatableHttpHeaders);
    }

    public static ValidationContext extractValidationContext(org.elasticsearch.http.HttpRequest request) {
        ValidatableHttpHeaders authenticatedHeaders = unwrapValidatableHeaders(request);
        return authenticatedHeaders != null ? authenticatedHeaders.validationContextSetOnce.get() : null;
    }

    private static ValidatableHttpHeaders unwrapValidatableHeaders(org.elasticsearch.http.HttpRequest request) {
        if (request instanceof Netty4HttpRequest == false) {
            return null;
        }
        if (((Netty4HttpRequest) request).getNettyRequest().headers() instanceof ValidatableHttpHeaders == false) {
            return null;
        }
        return (ValidatableHttpHeaders) (((Netty4HttpRequest) request).getNettyRequest().headers());
    }

    private static HttpPreRequest asHttpPreRequest(HttpRequest request) {
        return new HttpPreRequest() {

            @Override
            public RestRequest.Method method() {
                return Netty4HttpRequest.translateRequestMethod(request.method());
            }

            @Override
            public String uri() {
                return request.uri();
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                return Netty4HttpRequest.getHttpHeadersAsMap(request.headers());
            }
        };
    }

    public static final class ValidatableHttpHeaders extends DefaultHttpHeaders {

        @FunctionalInterface
        public interface ValidationContext {
            void assertValid();
        }

        public final SetOnce<ValidationContext> validationContextSetOnce = new SetOnce<>();

        public ValidatableHttpHeaders(HttpHeaders httpHeaders) {
            // the constructor implements the same logic as HttpHeaders#copy
            super();
            set(httpHeaders);
        }

        public void markValidationSucceeded(ValidationContext validationContext) {
            this.validationContextSetOnce.set(validationContext);
        }
    }
}
