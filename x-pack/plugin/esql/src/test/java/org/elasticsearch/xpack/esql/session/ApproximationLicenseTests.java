/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Who asked for approximation decides what an unlicensed cluster does about it. A user who asked gets the licensing
 * error; an operator's cluster-wide default simply does not apply, because the operator is not in the request path and
 * failing would break every query for people who never asked and cannot turn it off.
 */
public class ApproximationLicenseTests extends ESTestCase {

    private static final SettingsValidationContext CTX = new SettingsValidationContext(false, true);
    private static final String KEY = QuerySettingDef.CLUSTER_SETTING_PREFIX + "approximation";

    private static XPackLicenseState licensed() {
        return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null));
    }

    private static XPackLicenseState unlicensed() {
        return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(License.OperationMode.BASIC, true, null));
    }

    private static ResolvedSettings withOperatorDefault() {
        return QuerySettings.resolve(Settings.builder().put(KEY, "true").build(), Settings.EMPTY, Map.of(), null, CTX);
    }

    public void testOperatorDefaultIsDroppedOnAnUnlicensedCluster() {
        ResolvedSettings resolved = withOperatorDefault();
        assertThat(
            "precondition: the operator default applied",
            QuerySettings.APPROXIMATION.get(resolved),
            is(ApproximationSettings.DEFAULT)
        );

        ResolvedSettings settled = EsqlSession.applyApproximationLicense(resolved, new EsqlQueryRequest(), null, unlicensed());
        assertThat(QuerySettings.APPROXIMATION.get(settled), is(nullValue()));
    }

    public void testOperatorDefaultSurvivesOnALicensedCluster() {
        ResolvedSettings settled = EsqlSession.applyApproximationLicense(withOperatorDefault(), new EsqlQueryRequest(), null, licensed());
        assertThat(QuerySettings.APPROXIMATION.get(settled), is(ApproximationSettings.DEFAULT));
    }

    public void testUserSuppliedApproximationStillFailsOnAnUnlicensedCluster() {
        // Unchanged behaviour: the user asked for a paid feature this cluster does not have, and is told so.
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.set(QuerySettings.APPROXIMATION, ApproximationSettings.DEFAULT);
        ResolvedSettings resolved = QuerySettings.resolve(Settings.EMPTY, Settings.EMPTY, request.requestSettings(), null, CTX);

        var e = expectThrows(
            ElasticsearchStatusException.class,
            () -> EsqlSession.applyApproximationLicense(resolved, request, null, unlicensed())
        );
        assertThat(e.getMessage(), containsString("A valid Enterprise license is required to use ES|QL query approximation"));
    }

    public void testUserAskingOverAnOperatorDefaultStillFails() {
        // The user is still the one asking, even though an operator value is also present.
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.set(QuerySettings.APPROXIMATION, ApproximationSettings.DEFAULT);
        ResolvedSettings resolved = QuerySettings.resolve(
            Settings.builder().put(KEY, "true").build(),
            Settings.EMPTY,
            request.requestSettings(),
            null,
            CTX
        );

        expectThrows(
            ElasticsearchStatusException.class,
            () -> EsqlSession.applyApproximationLicense(resolved, request, null, unlicensed())
        );
    }

    public void testNothingHappensWhenApproximationIsOff() {
        ResolvedSettings resolved = QuerySettings.resolve(Settings.EMPTY, Settings.EMPTY, Map.of(), null, CTX);
        ResolvedSettings settled = EsqlSession.applyApproximationLicense(resolved, new EsqlQueryRequest(), null, unlicensed());
        assertThat(settled, equalTo(resolved));
    }
}
