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

public class Netty4HttpHeaderValidator {

    private final TriConsumer<HttpRequest, Channel, ActionListener<Void>> validator;

    public Netty4HttpHeaderValidator(TriConsumer<HttpRequest, Channel, ActionListener<Void>> validator) {
        this.validator = validator;
    }

    public Netty4HttpHeaderValidatorInboundHandler getValidatorInboundHandler() {
        return new Netty4HttpHeaderValidatorInboundHandler(validator);
    }

    public HttpMessage wrapNewlyDecodedMessage(HttpMessage newlyDecodedMessage) {
        DefaultHttpRequest httpRequest = (DefaultHttpRequest) newlyDecodedMessage;
        ValidatableHttpHeaders validatableHttpHeaders = new ValidatableHttpHeaders(newlyDecodedMessage.headers());
        return new DefaultHttpRequest(httpRequest.protocolVersion(), httpRequest.method(), httpRequest.uri(), validatableHttpHeaders);
    }

    public static final class ValidatableHttpHeaders extends DefaultHttpHeaders {

        interface ValidationContext {
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
