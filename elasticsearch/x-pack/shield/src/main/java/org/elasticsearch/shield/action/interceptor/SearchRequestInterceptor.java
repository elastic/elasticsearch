/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.interceptor;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequest;

/**
 * If field level security is enabled this interceptor disables the request cache for search requests.
 */
public class SearchRequestInterceptor extends FieldSecurityRequestInterceptor<SearchRequest> {

    @Inject
    public SearchRequestInterceptor(Settings settings) {
        super(settings);
    }

    @Override
    public void disableFeatures(SearchRequest request) {
        request.requestCache(false);
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof SearchRequest;
    }
}
