/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

public class LicenseFIPSTests extends AbstractLicenseServiceTestCase {

    public void testFIPSCheckWithAllowedLicense() throws Exception {
        License newLicense = TestUtils.generateSignedLicense(randomFrom("trial", "platinum"), TimeValue.timeValueHours(24L));
        PutLicenseRequest request = new PutLicenseRequest();
        request.acknowledge(true);
        request.license(newLicense);
        Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.fips_mode.enabled", randomBoolean())
            .build();
        XPackLicenseState licenseState = new XPackLicenseState(() -> 0);

        setInitialState(null, licenseState, settings);
        licenseService.start();
        PlainActionFuture<PutLicenseResponse> responseFuture = new PlainActionFuture<>();
        licenseService.registerLicense(request, responseFuture);
        if (responseFuture.isDone()) {
            // If the future is done, it means request/license validation failed.
            // In which case, this `actionGet` should throw a more useful exception than the verify below.
            responseFuture.actionGet();
        }
        verify(clusterService).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));
    }

    public void testFIPSCheckWithoutAllowedLicense() throws Exception {
        License newLicense = TestUtils.generateSignedLicense(randomFrom("gold", "standard"), TimeValue.timeValueHours(24L));
        PutLicenseRequest request = new PutLicenseRequest();
        request.acknowledge(true);
        request.license(newLicense);
        Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.fips_mode.enabled", true)
            .build();
        XPackLicenseState licenseState = new XPackLicenseState(() -> 0);

        setInitialState(null, licenseState, settings);
        licenseService.start();
        PlainActionFuture<PutLicenseResponse> responseFuture = new PlainActionFuture<>();
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> licenseService.registerLicense(request, responseFuture));
        assertThat(e.getMessage(),
            containsString("Cannot install a [" + newLicense.operationMode() + "] license unless FIPS mode is disabled"));
        licenseService.stop();

        settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.fips_mode.enabled", false)
            .build();
        licenseState = new XPackLicenseState(() -> 0);

        setInitialState(null, licenseState, settings);
        licenseService.start();
        licenseService.registerLicense(request, responseFuture);
        if (responseFuture.isDone()) {
            // If the future is done, it means request/license validation failed.
            // In which case, this `actionGet` should throw a more useful exception than the verify below.
            responseFuture.actionGet();
        }
        verify(clusterService).submitStateUpdateTask(any(String.class), any(ClusterStateUpdateTask.class));
    }
}
