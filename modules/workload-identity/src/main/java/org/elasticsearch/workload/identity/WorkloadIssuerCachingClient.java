/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workload.identity;

import java.util.Map;

/**
 * A client for Workload Identity Issuer, which issues short-lived workload identity tokens (JWT)
 * that other plugins can later exchange for cloud-provider-specific credentials.
 */
public interface WorkloadIssuerCachingClient {

    String issueToken(String audience, Map<String, String> customClaims);
}
