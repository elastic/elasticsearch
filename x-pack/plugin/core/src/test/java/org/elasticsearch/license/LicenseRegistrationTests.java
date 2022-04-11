/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.UUID;

import static org.elasticsearch.license.TestUtils.dateMath;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LicenseRegistrationTests extends AbstractLicenseServiceTestCase {

    public void testSelfGeneratedTrialLicense() throws Exception {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        setInitialState(null, licenseState, Settings.EMPTY, "trial");
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        licenseService.start();

        ClusterState state = ClusterState.builder(new ClusterName("a")).build();
        ArgumentCaptor<ClusterStateUpdateTask> stateUpdater = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, Mockito.times(1)).submitStateUpdateTask(any(), stateUpdater.capture(), any());
        ClusterState stateWithLicense = stateUpdater.getValue().execute(state);
        LicensesMetadata licenseMetadata = stateWithLicense.metadata().custom(LicensesMetadata.TYPE);
        assertNotNull(licenseMetadata);
        assertNotNull(licenseMetadata.getLicense());
        assertFalse(licenseMetadata.isEligibleForTrial());
        assertEquals("trial", licenseMetadata.getLicense().type());
        assertEquals(
            clock.millis() + LicenseService.NON_BASIC_SELF_GENERATED_LICENSE_DURATION.millis(),
            licenseMetadata.getLicense().expiryDate()
        );
    }

    public void testSelfGeneratedBasicLicense() throws Exception {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        setInitialState(null, licenseState, Settings.EMPTY, "basic");
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        licenseService.start();

        ClusterState state = ClusterState.builder(new ClusterName("a")).build();
        ArgumentCaptor<ClusterStateUpdateTask> stateUpdater = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, Mockito.times(1)).submitStateUpdateTask(any(), stateUpdater.capture(), any());
        ClusterState stateWithLicense = stateUpdater.getValue().execute(state);
        LicensesMetadata licenseMetadata = stateWithLicense.metadata().custom(LicensesMetadata.TYPE);
        assertNotNull(licenseMetadata);
        assertNotNull(licenseMetadata.getLicense());
        assertTrue(licenseMetadata.isEligibleForTrial());
        assertEquals("basic", licenseMetadata.getLicense().type());
        assertEquals(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, licenseMetadata.getLicense().expiryDate());
    }

    public void testNonSelfGeneratedBasicLicenseIsReplaced() throws Exception {
        long now = System.currentTimeMillis();
        String uid = UUID.randomUUID().toString();
        final License.Builder builder = License.builder()
            .uid(uid)
            .version(License.VERSION_CURRENT)
            .expiryDate(dateMath("now+2h", now))
            .startDate(now)
            .issueDate(now)
            .type("basic")
            .issuedTo("customer")
            .issuer("elasticsearch")
            .maxNodes(5);
        License license = TestUtils.generateSignedLicense(builder);

        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        setInitialState(license, licenseState, Settings.EMPTY);
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        licenseService.start();

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.putCustom(LicensesMetadata.TYPE, new LicensesMetadata(license, null));
        ClusterState state = ClusterState.builder(new ClusterName("a")).metadata(mdBuilder.build()).build();
        ArgumentCaptor<ClusterStateUpdateTask> stateUpdater = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, Mockito.times(1)).submitStateUpdateTask(any(), stateUpdater.capture(), any());
        ClusterState stateWithLicense = stateUpdater.getValue().execute(state);
        LicensesMetadata licenseMetadata = stateWithLicense.metadata().custom(LicensesMetadata.TYPE);
        assertNotNull(licenseMetadata);
        assertNotNull(licenseMetadata.getLicense());
        assertTrue(licenseMetadata.isEligibleForTrial());
        assertEquals("basic", licenseMetadata.getLicense().type());
        assertEquals(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, licenseMetadata.getLicense().expiryDate());
        assertEquals(uid, licenseMetadata.getLicense().uid());
    }

    public void testExpiredSelfGeneratedBasicLicenseIsExtended() throws Exception {
        long now = System.currentTimeMillis();
        String uid = UUID.randomUUID().toString();
        License.Builder builder = License.builder()
            .uid(uid)
            .issuedTo("name")
            .maxNodes(1000)
            .issueDate(dateMath("now-10h", now))
            .type("basic")
            .expiryDate(dateMath("now-2h", now));
        License license = SelfGeneratedLicense.create(builder, License.VERSION_CURRENT);

        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        setInitialState(license, licenseState, Settings.EMPTY);
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        licenseService.start();

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.putCustom(LicensesMetadata.TYPE, new LicensesMetadata(license, null));
        ClusterState state = ClusterState.builder(new ClusterName("a")).metadata(mdBuilder.build()).build();
        ArgumentCaptor<ClusterStateUpdateTask> stateUpdater = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, Mockito.times(1)).submitStateUpdateTask(any(), stateUpdater.capture(), any());
        ClusterState stateWithLicense = stateUpdater.getValue().execute(state);
        LicensesMetadata licenseMetadata = stateWithLicense.metadata().custom(LicensesMetadata.TYPE);
        assertNotNull(licenseMetadata);
        assertNotNull(licenseMetadata.getLicense());
        assertTrue(licenseMetadata.isEligibleForTrial());
        assertEquals("basic", licenseMetadata.getLicense().type());
        assertEquals(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, licenseMetadata.getLicense().expiryDate());
        assertEquals(uid, licenseMetadata.getLicense().uid());
    }
}
