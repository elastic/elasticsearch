/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.shield.User;
import org.elasticsearch.transport.TransportRequest;

/**
 *
 */
public interface AuthorizationService {

    void authorize(User user, String action, TransportRequest request) throws AuthorizationException;

}
