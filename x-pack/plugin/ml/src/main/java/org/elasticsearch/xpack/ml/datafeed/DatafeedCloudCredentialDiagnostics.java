/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorUtils;

/**
 * Actionable user-facing messages when a CPS datafeed's internal cloud API key fails during search.
 */
public final class DatafeedCloudCredentialDiagnostics {

    private DatafeedCloudCredentialDiagnostics() {}

    public static Exception enrichIfCloudCredentialFailure(
        @Nullable String cloudCredentialId,
        DataExtractorUtils.CloudCredentialFailureKind kind,
        Exception failure
    ) {
        if (kind == DataExtractorUtils.CloudCredentialFailureKind.NONE || cloudCredentialId == null) {
            return failure;
        }
        String auditTemplate = switch (kind) {
            case AUTHENTICATION -> Messages.JOB_AUDIT_DATAFEED_CPS_KEY_RUNTIME_FAILURE;
            case AUTHORIZATION -> Messages.JOB_AUDIT_DATAFEED_CPS_KEY_RUNTIME_AUTHZ_FAILURE;
            case NONE -> throw new AssertionError("unreachable");
        };
        Exception enriched = new Exception(Messages.getMessage(auditTemplate, cloudCredentialId));
        enriched.initCause(failure);
        return enriched;
    }
}
