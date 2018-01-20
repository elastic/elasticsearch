/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.transport.TransportRequest;

/**
 * A request interceptor can introspect a request and modify it.
 */
public interface RequestInterceptor<Request> {

    /**
     * If {@link #supports(TransportRequest)} returns <code>true</code> this interceptor will introspect the request
     * and potentially modify it.
     */
    void intercept(Request request, User user, Role userPermissions, String action);

    /**
     * Returns whether this request interceptor should intercept the specified request.
     */
    boolean supports(TransportRequest request);

}
