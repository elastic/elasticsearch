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
import org.elasticsearch.http.HttpHeadersValidationException;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.netty4.HttpHeadersValidator.ValidatableHttpHeaders.ValidationResultContext;
import org.elasticsearch.rest.RestRequest;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.http.netty4.Netty4HttpRequest.getHttpHeadersAsMap;
import static org.elasticsearch.http.netty4.Netty4HttpRequest.translateRequestMethod;

public final class HttpHeadersValidator {

    /**
     * Trivial {@link HttpHeadersValidator} implementation, to be used in tests, that successfully validates
     * any and all HTTP request headers.
     */
    public static final HttpHeadersValidator VALIDATE_EVERYTHING_VALIDATOR = new HttpHeadersValidator(
        (httpPreRequest, channel, listener) -> listener.onResponse(ValidatableHttpHeaders.OK)
    );

    /**
     * An async HTTP headers validator function that receives as arguments part of the incoming HTTP request
     * (except the body contents, see {@link HttpPreRequest}), as well as the netty channel that the request is
     * being received over, and must then call the {@code ActionListener#onResponse} method on the listener parameter
     * in case the validation is to be considered successful, or otherwise call {@code ActionListener#onFailure}.
     */
    private final TriConsumer<HttpPreRequest, Channel, ActionListener<ValidationResultContext>> validator;

    public HttpHeadersValidator(TriConsumer<HttpPreRequest, Channel, ActionListener<ValidationResultContext>> validator) {
        this.validator = validator;
    }

    public Netty4HttpHeaderValidator getValidatorInboundHandler() {
        return new Netty4HttpHeaderValidator((httpRequest, channel, listener) -> {
            if (httpRequest.headers() instanceof ValidatableHttpHeaders validatableHttpHeaders) {
                // make sure validation only runs on properly wrapped "validatable" headers implementation
                validator.apply(asHttpPreRequest(httpRequest), channel, ActionListener.wrap(validationResultContext -> {
                    validatableHttpHeaders.markAsSuccessfullyValidated(validationResultContext);
                    // a successful validation needs to signal to the {@link Netty4HttpHeaderValidator} to resume
                    // forwarding the request beyond the headers part
                    listener.onResponse(null);
                }, e -> listener.onFailure(new HttpHeadersValidationException(e))));
            } else {
                listener.onFailure(new IllegalStateException("Expected a validatable request for validation"));
            }
        });
    }

    public HttpMessage wrapAsValidatableMessage(HttpMessage newlyDecodedMessage) {
        DefaultHttpRequest httpRequest = (DefaultHttpRequest) newlyDecodedMessage;
        ValidatableHttpHeaders validatableHttpHeaders = new ValidatableHttpHeaders(newlyDecodedMessage.headers());
        return new DefaultHttpRequest(httpRequest.protocolVersion(), httpRequest.method(), httpRequest.uri(), validatableHttpHeaders);
    }

    public static ValidationResultContext extractValidationContext(org.elasticsearch.http.HttpRequest request) {
        ValidatableHttpHeaders authenticatedHeaders = unwrapValidatableHeaders(request);
        return authenticatedHeaders != null ? authenticatedHeaders.validationResultContextSetOnce.get() : null;
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

    /**
     * {@link HttpHeaders} implementation that carries along the {@link ValidationResultContext} iff
     * the HTTP headers have been validated successfully.
     */
    public static final class ValidatableHttpHeaders extends DefaultHttpHeaders {

        static final ValidationResultContext OK = () -> {};

        @FunctionalInterface
        public interface ValidationResultContext {
            void assertValid();
        }

        public final SetOnce<ValidationResultContext> validationResultContextSetOnce;

        public ValidatableHttpHeaders(HttpHeaders httpHeaders) {
            this(httpHeaders, new SetOnce<>());
        }

        private ValidatableHttpHeaders(HttpHeaders httpHeaders, SetOnce<ValidationResultContext> validationResultContextSetOnce) {
            // the constructor implements the same logic as HttpHeaders#copy
            super();
            set(httpHeaders);
            this.validationResultContextSetOnce = validationResultContextSetOnce;
        }

        private ValidatableHttpHeaders(HttpHeaders httpHeaders, ValidationResultContext validationResultContext) {
            this(httpHeaders);
            if (validationResultContext != null) {
                markAsSuccessfullyValidated(validationResultContext);
            }
        }

        /**
         * Must be called at most once in order to mark the http headers as successfully validated.
         * The intent of the {@link ValidationResultContext} parameter is to associate some details about the validation
         * that can be later retrieved when dispatching the request.
         */
        public void markAsSuccessfullyValidated(ValidationResultContext validationResultContext) {
            this.validationResultContextSetOnce.set(Objects.requireNonNull(validationResultContext));
        }

        @Override
        public HttpHeaders copy() {
            // copy the headers but also STILL CARRY the same validation result
            return new ValidatableHttpHeaders(super.copy(), validationResultContextSetOnce.get());
        }
    }

    /**
     * Translates the netty request internal type to a {@link HttpPreRequest} instance that code outside the network plugin has access to.
     */
    private static HttpPreRequest asHttpPreRequest(HttpRequest request) {
        return new HttpPreRequest() {

            @Override
            public RestRequest.Method method() {
                return translateRequestMethod(request.method());
            }

            @Override
            public String uri() {
                return request.uri();
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                return getHttpHeadersAsMap(request.headers());
            }
        };
    }
}
