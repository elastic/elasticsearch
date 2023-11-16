/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.apache.http.client.methods.HttpUriRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequestEntity;

import java.util.List;

import static org.elasticsearch.core.Strings.format;

public record HuggingFaceElserRequestCreator(HuggingFaceAccount account) implements RequestCreator<HuggingFaceAccount> {

    private static final Logger logger = LogManager.getLogger(HuggingFaceElserRequestCreator.class);

    @Override
    public Runnable create(List<String> input, Components components, ActionListener<HttpResult> listener) {
        var a = new HuggingFaceElserRequest(account, new HuggingFaceElserRequestEntity(input));

        return components.threadPool().getThreadContext().preserveContext(new Command(components, a.createRequest(), listener));
    }

    @Override
    public HuggingFaceAccount key() {
        return account;
    }

    private record Command(Components components, HttpUriRequest request, ActionListener<HttpResult> listener) implements Runnable {
        @Override
        public void run() {
            try {
                components.httpClient().send(request, components.context(), listener);
            } catch (Exception e) {
                logger.warn(format("Failed to send request [%s] via the http client", request.getRequestLine()), e);
                listener.onFailure(new ElasticsearchException(format("Failed to send request [%s]", request.getRequestLine()), e));
            }
        }
    }
}
