/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class LicensesManagerServiceTests extends ESSingleNodeTestCase {
    static {
        MetaData.registerPrototype(LicensesMetaData.TYPE, LicensesMetaData.PROTO);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testStoreAndGetLicenses() throws Exception {
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        License goldLicense = generateSignedLicense("gold", TimeValue.timeValueHours(1));
        TestUtils.registerAndAckSignedLicenses(licensesService, goldLicense, LicensesStatus.VALID);
        License silverLicense = generateSignedLicense("silver", TimeValue.timeValueHours(2));
        TestUtils.registerAndAckSignedLicenses(licensesService, silverLicense, LicensesStatus.VALID);
        License platinumLicense = generateSignedLicense("platinum", TimeValue.timeValueHours(1));
        TestUtils.registerAndAckSignedLicenses(licensesService, platinumLicense, LicensesStatus.VALID);
        License basicLicense = generateSignedLicense("basic", TimeValue.timeValueHours(3));
        TestUtils.registerAndAckSignedLicenses(licensesService, basicLicense, LicensesStatus.VALID);
        LicensesMetaData licensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesMetaData.getLicense(), equalTo(basicLicense));
        final License getLicenses = licensesService.getLicense();
        assertThat(getLicenses, equalTo(basicLicense));
    }

    public void testEffectiveLicenses() throws Exception {
        final LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        License goldLicense = generateSignedLicense("gold", TimeValue.timeValueSeconds(5));
        // put gold license
        TestUtils.registerAndAckSignedLicenses(licensesService, goldLicense, LicensesStatus.VALID);
        LicensesMetaData licensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesService.getLicense(licensesMetaData), equalTo(goldLicense));

        License platinumLicense = generateSignedLicense("platinum", TimeValue.timeValueSeconds(3));
        // put platinum license
        TestUtils.registerAndAckSignedLicenses(licensesService, platinumLicense, LicensesStatus.VALID);
        licensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesService.getLicense(licensesMetaData), equalTo(platinumLicense));

        License basicLicense = generateSignedLicense("basic", TimeValue.timeValueSeconds(3));
        // put basic license
        TestUtils.registerAndAckSignedLicenses(licensesService, basicLicense, LicensesStatus.VALID);
        licensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesService.getLicense(licensesMetaData), equalTo(basicLicense));
    }

    public void testInvalidLicenseStorage() throws Exception {
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        License signedLicense = generateSignedLicense(TimeValue.timeValueMinutes(2));

        // modify content of signed license
        License tamperedLicense = License.builder()
                .fromLicenseSpec(signedLicense, signedLicense.signature())
                .expiryDate(signedLicense.expiryDate() + 10 * 24 * 60 * 60 * 1000L)
                .validate()
                .build();

        TestUtils.registerAndAckSignedLicenses(licensesService, tamperedLicense, LicensesStatus.INVALID);

        // ensure that the invalid license never made it to cluster state
        LicensesMetaData licensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        if (licensesMetaData != null) {
            assertThat(licensesMetaData.getLicense(), equalTo(LicensesMetaData.LICENSE_TOMBSTONE));
        }
    }

    public void testRemoveLicenses() throws Exception {
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        // generate a trial license for one feature
        licensesService.register(new TestUtils.AssertingLicensee("", logger));

        // generate signed licenses
        License license = generateSignedLicense(TimeValue.timeValueHours(1));
        TestUtils.registerAndAckSignedLicenses(licensesService, license, LicensesStatus.VALID);
        LicensesMetaData licensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesMetaData.getLicense(), not(LicensesMetaData.LICENSE_TOMBSTONE));

        // remove signed licenses
        removeAndAckSignedLicenses(licensesService);
        licensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesMetaData.getLicense(), equalTo(LicensesMetaData.LICENSE_TOMBSTONE));
    }

    private void removeAndAckSignedLicenses(final LicensesService licensesService) {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(false);
        licensesService.removeLicense(new DeleteLicenseRequest(), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                if (clusterStateUpdateResponse.isAcknowledged()) {
                    success.set(true);
                }
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable throwable) {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        assertThat("remove license(s) failed", success.get(), equalTo(true));
    }
}
