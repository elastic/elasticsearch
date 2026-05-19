/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.workload;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.Map;

class WorkloadIssuerCachingClient {

    private static final String WORKLOAD_ISSUER_SSL_CONFIGURATION_PREFIX = "xpack.security.workload_identity_issuer_client.ssl.";

    WorkloadIssuerCachingClient(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        SSLService sslService,
        TimeValue connectionTtl
    ) {}

    public String issueToken(String aud, Map<String, String> customClaims) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
