/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

/**
 * Marker exception signaled by {@link org.elasticsearch.xpack.inference.action.TransportElasticInferenceServiceAuthorizationAction}
 * when CCM is supported in this environment but currently disabled.
 * <p>
 * The {@code AuthorizationPoller}'s failure handler recognizes this exception and calls
 * {@code shutdownInternal(markAsCompleted)} to complete the persistent task, preserving the
 * self-stop backstop that {@code CCMService.disableCCM()} relies on when the DISABLE broadcast's
 * stop request fails.
 * <p>
 * This exception is used for local node signaling only and never crosses the wire.
 */
public class CcmDisabledException extends Exception {

    public CcmDisabledException() {
        super("CCM is disabled", null, true, false);
    }
}
