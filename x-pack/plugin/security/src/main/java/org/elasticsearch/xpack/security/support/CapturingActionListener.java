/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.action.ActionListener;

/**
 * Simple implementation of ActionListener to capture response or exception.
 * Allowed a caller to pass this listener and get a response.
 * @param <T> Response class
 */
public class CapturingActionListener<T> implements ActionListener<T> {
    private T response = null;
    private Exception failure = null;

    @Override
    public void onResponse(final T result) {
        this.response = result;
    }

    @Override
    public void onFailure(final Exception exception) {
        this.failure = exception;
    }

    public T getResponse() {
        return this.response;
    }

    public Exception getFailure() {
        return this.failure;
    }
}
