/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.UUID;

import static org.elasticsearch.license.TestUtils.dateMath;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LicenseRegistrationTests extends AbstractLicenseServiceTestCase {

    public void testSelfGeneratedTrialLicense() throws Exception {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        setInitialState(null, licenseState, Settings.EMPTY, "trial");
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        licenseService.start();

        ClusterState state = ClusterState.builder(new ClusterName("a")).build();
        ArgumentCaptor<ClusterStateUpdateTask> stateUpdater = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, Mockito.times(1)).submitStateUpdateTask(any(), stateUpdater.capture());
        ClusterState stateWithLicense = stateUpdater.getValue().execute(state);
        LicensesMetaData licenseMetaData = stateWithLicense.metaData().custom(LicensesMetaData.TYPE);
        assertNotNull(licenseMetaData);
        assertNotNull(licenseMetaData.getLicense());
        assertFalse(licenseMetaData.isEligibleForTrial());
        assertEquals("trial", licenseMetaData.getLicense().type());
        assertEquals(clock.millis() + LicenseService.NON_BASIC_SELF_GENERATED_LICENSE_DURATION.millis(),
                licenseMetaData.getLicense().expiryDate());
    }

    public void testSelfGeneratedBasicLicense() throws Exception {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        setInitialState(null, licenseState, Settings.EMPTY, "basic");
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        licenseService.start();

        ClusterState state = ClusterState.builder(new ClusterName("a")).build();
        ArgumentCaptor<ClusterStateUpdateTask> stateUpdater = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, Mockito.times(1)).submitStateUpdateTask(any(), stateUpdater.capture());
        ClusterState stateWithLicense = stateUpdater.getValue().execute(state);
        LicensesMetaData licenseMetaData = stateWithLicense.metaData().custom(LicensesMetaData.TYPE);
        assertNotNull(licenseMetaData);
        assertNotNull(licenseMetaData.getLicense());
        assertTrue(licenseMetaData.isEligibleForTrial());
        assertEquals("basic", licenseMetaData.getLicense().type());
        assertEquals(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, licenseMetaData.getLicense().expiryDate());
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

        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        setInitialState(license, licenseState, Settings.EMPTY);
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        licenseService.start();

        MetaData.Builder mdBuilder = MetaData.builder();
        mdBuilder.putCustom(LicensesMetaData.TYPE, new LicensesMetaData(license, null));
        ClusterState state = ClusterState.builder(new ClusterName("a")).metaData(mdBuilder.build()).build();
        ArgumentCaptor<ClusterStateUpdateTask> stateUpdater = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, Mockito.times(1)).submitStateUpdateTask(any(), stateUpdater.capture());
        ClusterState stateWithLicense = stateUpdater.getValue().execute(state);
        LicensesMetaData licenseMetaData = stateWithLicense.metaData().custom(LicensesMetaData.TYPE);
        assertNotNull(licenseMetaData);
        assertNotNull(licenseMetaData.getLicense());
        assertTrue(licenseMetaData.isEligibleForTrial());
        assertEquals("basic", licenseMetaData.getLicense().type());
        assertEquals(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, licenseMetaData.getLicense().expiryDate());
        assertEquals(uid, licenseMetaData.getLicense().uid());
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

        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        setInitialState(license, licenseState, Settings.EMPTY);
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        licenseService.start();

        MetaData.Builder mdBuilder = MetaData.builder();
        mdBuilder.putCustom(LicensesMetaData.TYPE, new LicensesMetaData(license, null));
        ClusterState state = ClusterState.builder(new ClusterName("a")).metaData(mdBuilder.build()).build();
        ArgumentCaptor<ClusterStateUpdateTask> stateUpdater = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, Mockito.times(1)).submitStateUpdateTask(any(), stateUpdater.capture());
        ClusterState stateWithLicense = stateUpdater.getValue().execute(state);
        LicensesMetaData licenseMetaData = stateWithLicense.metaData().custom(LicensesMetaData.TYPE);
        assertNotNull(licenseMetaData);
        assertNotNull(licenseMetaData.getLicense());
        assertTrue(licenseMetaData.isEligibleForTrial());
        assertEquals("basic", licenseMetaData.getLicense().type());
        assertEquals(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, licenseMetaData.getLicense().expiryDate());
        assertEquals(uid, licenseMetaData.getLicense().uid());
    }
}
