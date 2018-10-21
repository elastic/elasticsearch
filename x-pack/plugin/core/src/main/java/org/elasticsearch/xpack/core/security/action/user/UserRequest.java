/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

/**
 * Interface for requests that involve user operations
 */
public interface UserRequest {

    /**
     * Accessor for the usernames that this request pertains to. <code>null</code> should never be returned!
     */
    String[] usernames();
}
