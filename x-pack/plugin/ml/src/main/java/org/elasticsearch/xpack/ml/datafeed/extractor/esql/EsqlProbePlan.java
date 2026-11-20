/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;

import java.util.Objects;

/** Decides whether a datafeed update needs the ES|QL {@code DESCRIBE} probe re-run. */
public final class EsqlProbePlan {

    private EsqlProbePlan() {}

    /**
     * An ES|QL datafeed update needs the probe when its project routing or credential changed, or when it newly requires a summary
     * count column. Other updates cannot change the probe outcome. A PUT always probes and does not use this method.
     */
    public static boolean needsProbe(DatafeedConfig current, DatafeedConfig updated, Job job) {
        if (updated.getEsqlQuery() == null) {
            return false;
        }
        boolean searchScopeChanged = Objects.equals(current.getProjectRouting(), updated.getProjectRouting()) == false
            || Objects.equals(credentialId(current), credentialId(updated)) == false;
        if (searchScopeChanged) {
            return true;
        }
        String requiredBefore = EsqlDatafeedQueryValidator.requiredSummaryCountField(current, job);
        String requiredAfter = EsqlDatafeedQueryValidator.requiredSummaryCountField(updated, job);
        return requiredAfter != null && Objects.equals(requiredBefore, requiredAfter) == false;
    }

    private static String credentialId(DatafeedConfig config) {
        PersistedCloudCredential credential = config.getCloudInternalCredential();
        return credential == null ? null : credential.id();
    }
}
