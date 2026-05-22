/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workload.identity;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

class WorkloadIssuerCachingClientImpl implements WorkloadIssuerCachingClient {

    WorkloadIssuerCachingClientImpl(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        SslConfiguration sslConfiguration,
        TimeValue connectionTtl
    ) {}

    @Override
    public String issueToken(String audience, Map<String, String> customClaims) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
