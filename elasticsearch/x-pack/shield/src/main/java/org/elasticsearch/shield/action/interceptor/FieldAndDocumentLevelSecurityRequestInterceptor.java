/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.interceptor;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authz.InternalAuthorizationService;
import org.elasticsearch.shield.authz.accesscontrol.IndicesAccessControl;

import java.util.Collections;
import java.util.List;

/**
 * Base class for interceptors that disables features when field level security is configured for indices a request
 * is going to execute on.
 */
public abstract class FieldAndDocumentLevelSecurityRequestInterceptor<Request> extends AbstractComponent implements
        RequestInterceptor<Request> {

    private final ThreadContext threadContext;

    public FieldAndDocumentLevelSecurityRequestInterceptor(Settings settings, ThreadContext threadContext) {
        super(settings);
        this.threadContext = threadContext;
    }

    public void intercept(Request request, User user) {
        List<? extends IndicesRequest> indicesRequests;
        if (request instanceof CompositeIndicesRequest) {
            indicesRequests = ((CompositeIndicesRequest) request).subRequests();
        } else if (request instanceof IndicesRequest) {
            indicesRequests = Collections.singletonList((IndicesRequest) request);
        } else {
            throw new IllegalArgumentException(LoggerMessageFormat.format("Expected a request of type [{}] or [{}] but got [{}] instead",
                    CompositeIndicesRequest.class, IndicesRequest.class, request.getClass()));
        }
        IndicesAccessControl indicesAccessControl = threadContext.getTransient(InternalAuthorizationService.INDICES_PERMISSIONS_KEY);
        for (IndicesRequest indicesRequest : indicesRequests) {
            for (String index : indicesRequest.indices()) {
                IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index);
                if (indexAccessControl != null) {
                    boolean fls = indexAccessControl.getFields() != null;
                    boolean dls = indexAccessControl.getQueries() != null;
                    if (fls || dls) {
                        logger.debug("intercepted request for index [{}] with field level or document level security enabled, " +
                                "disabling features", index);
                        disableFeatures(request);
                        return;
                    }
                }
                logger.trace("intercepted request for index [{}] with neither field level or document level security not enabled, "
                        + "doing nothing", index);
            }
        }
    }

    protected abstract void disableFeatures(Request request);

}
