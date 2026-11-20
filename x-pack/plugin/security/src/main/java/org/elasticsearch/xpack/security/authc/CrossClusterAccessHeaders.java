/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;

import java.io.IOException;

/**
 * Outbound cross-cluster access headers, written by the originating cluster when sending a request to a remote cluster.
 * The receiving side reads them back with {@link CrossClusterAccessRequestHeaders}, which deliberately keeps the subject info
 * encoded until the API key has been authenticated.
 */
public final class CrossClusterAccessHeaders {

    public static final String CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY = "_cross_cluster_access_credentials";
    private final String credentialsHeader;
    private final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo;

    public CrossClusterAccessHeaders(String credentialsHeader, CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo) {
        assert credentialsHeader.startsWith("ApiKey ") : "credentials header must start with [ApiKey ]";
        this.credentialsHeader = credentialsHeader;
        this.crossClusterAccessSubjectInfo = crossClusterAccessSubjectInfo;
    }

    public void writeToContext(final ThreadContext ctx) throws IOException {
        ctx.putHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, credentialsHeader);
        crossClusterAccessSubjectInfo.writeToContext(ctx);
    }
}
