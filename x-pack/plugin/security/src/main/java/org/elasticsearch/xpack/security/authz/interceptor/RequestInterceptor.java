/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;

/**
 * A request interceptor can introspect a request and modify it.
 */
public interface RequestInterceptor {

    /**
     * This interceptor will introspect the request and potentially modify it. If the interceptor does not apply
     * to the request then the request will not be modified.
     */
    void intercept(RequestInfo requestInfo, AuthorizationEngine authorizationEngine, AuthorizationInfo authorizationInfo,
                   ActionListener<Void> listener);
}
