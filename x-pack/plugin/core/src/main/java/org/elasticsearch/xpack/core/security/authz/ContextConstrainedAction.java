/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz;

/**
 * An interface for {@link org.elasticsearch.action.ActionType} subclasses that restricts
 * when the action can be executed. Actions implementing this interface declare a required
 * invocation context; the authorization service will deny execution unless the matching
 * context marker is present in the thread context.
 */
public interface ContextConstrainedAction {

    /**
     * The thread-context header key used to carry the invocation context marker.
     */
    String HEADER_KEY = "_xpack_security_internal_action_context";

    /**
     * Returns the invocation context required to execute this action.
     * The caller must set a thread-context header ({@link #HEADER_KEY})
     * with this value before dispatching the action.
     */
    String requiredInvocationContext();
}
