/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import java.util.List;
import java.util.Objects;

public abstract class FilterRestHandler implements RestHandler {
    private final RestHandler delegate;

    protected FilterRestHandler(RestHandler delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    protected RestHandler getDelegate() {
        return delegate;
    }

    @Override
    public RestHandler getConcreteRestHandler() {
        return delegate.getConcreteRestHandler();
    }

    @Override
    public List<RestHandler.Route> routes() {
        return delegate.routes();
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return delegate.allowSystemIndexAccessByDefault();
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return delegate.canTripCircuitBreaker();
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return delegate.allowsUnsafeBuffers();
    }

    @Override
    public boolean supportsBulkContent() {
        return delegate.supportsBulkContent();
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return delegate.mediaTypesValid(request);
    }
}
