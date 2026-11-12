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

/** Which ES|QL {@code LIMIT 0} probes a datafeed validation runs: required columns and {@code source_time_field} type. */
public record EsqlProbePlan(boolean columns, boolean sourceTimeField) {

    public static final EsqlProbePlan ALL = new EsqlProbePlan(true, true);
    private static final EsqlProbePlan NONE = new EsqlProbePlan(false, false);

    /** Both probes read project routing and credential; only the columns probe reads the required summary count field. */
    public static EsqlProbePlan forUpdate(DatafeedConfig current, DatafeedConfig updated, Job job) {
        if (updated.getEsqlQuery() == null) {
            return NONE;
        }
        boolean searchScopeChanged = Objects.equals(current.getProjectRouting(), updated.getProjectRouting()) == false
            || Objects.equals(credentialId(current), credentialId(updated)) == false;
        if (searchScopeChanged) {
            return ALL;
        }
        String requiredBefore = EsqlDatafeedQueryValidator.requiredSummaryCountField(current, job);
        String requiredAfter = EsqlDatafeedQueryValidator.requiredSummaryCountField(updated, job);
        return new EsqlProbePlan(requiredAfter != null && Objects.equals(requiredBefore, requiredAfter) == false, false);
    }

    private static String credentialId(DatafeedConfig config) {
        PersistedCloudCredential credential = config.getCloudInternalCredential();
        return credential == null ? null : credential.id();
    }
}
