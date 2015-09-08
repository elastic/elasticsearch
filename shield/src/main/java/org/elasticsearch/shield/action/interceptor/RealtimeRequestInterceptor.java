/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.interceptor;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authz.InternalAuthorizationService;
import org.elasticsearch.shield.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.transport.TransportRequest;

import java.util.Collections;
import java.util.List;

/**
 * If field level or document level security is enabled this interceptor disables the realtime feature of get, multi get, termsvector and
 * multi termsvector requests.
 */
public class RealtimeRequestInterceptor extends AbstractComponent implements RequestInterceptor<RealtimeRequest> {

    @Inject
    public RealtimeRequestInterceptor(Settings settings) {
        super(settings);
    }

    @Override
    public void intercept(RealtimeRequest request, User user) {
        List<? extends IndicesRequest> indicesRequests;
        if (request instanceof CompositeIndicesRequest) {
            indicesRequests = ((CompositeIndicesRequest) request).subRequests();
        } else if (request instanceof IndicesRequest) {
            indicesRequests = Collections.singletonList((IndicesRequest) request);
        } else {
            throw new IllegalArgumentException(LoggerMessageFormat.format("Expected a request of type [{}] or [{}] but got [{}] instead", CompositeIndicesRequest.class, IndicesRequest.class, request.getClass()));
        }
        IndicesAccessControl indicesAccessControl = ((TransportRequest) request).getFromContext(InternalAuthorizationService.INDICES_PERMISSIONS_KEY);
        for (IndicesRequest indicesRequest : indicesRequests) {
            for (String index : indicesRequest.indices()) {
                IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index);
                if (indexAccessControl != null && (indexAccessControl.getFields() != null || indexAccessControl.getQueries() != null)) {
                    logger.debug("intercepted request for index [{}] with field level or document level security enabled, forcefully disabling realtime", index);
                    request.realtime(false);
                    return;
                } else {
                    logger.trace("intercepted request for index [{}] with field level security and document level not enabled, doing nothing", index);
                }
            }
        }
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof RealtimeRequest;
    }
}
