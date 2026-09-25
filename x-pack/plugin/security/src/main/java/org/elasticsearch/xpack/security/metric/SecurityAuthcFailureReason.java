/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

/**
 * Low-cardinality failure reason recorded on authc failure metrics.
 */
public interface SecurityAuthcFailureReason {

    SecurityAuthcFailureReason CLIENT_AUTHENTICATION_FAILED = () -> "client.authentication_failed";

    SecurityAuthcFailureReason SERVER_INTERNAL_ERROR = () -> "server.internal_error";

    SecurityAuthcFailureReason SERVER_THREAD_POOL_REJECTED_EXECUTION = () -> "server.thread_pool_rejected_execution";

    String value();
}
