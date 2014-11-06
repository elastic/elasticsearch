/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.manager.ESLicenseManager;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.license.plugin.core.*;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 10)
public class LicensesManagerServiceTests extends AbstractLicensesServiceTests {

    @Test
    public void testStoreAndGetLicenses() throws Exception {
        LicensesManagerService licensesManagerService = masterLicensesManagerService();
        ESLicense shieldShortLicense = generateSignedLicense("shield", TimeValue.timeValueHours(1));
        ESLicense shieldLongLicense = generateSignedLicense("shield", TimeValue.timeValueHours(2));
        ESLicense marvelShortLicense = generateSignedLicense("marvel", TimeValue.timeValueHours(1));
        ESLicense marvelLongLicense = generateSignedLicense("marvel", TimeValue.timeValueHours(2));

        List<ESLicense> licenses = Arrays.asList(shieldLongLicense, shieldShortLicense, marvelLongLicense, marvelShortLicense);
        Collections.shuffle(licenses);
        registerAndAckSignedLicenses(licensesManagerService, licenses, LicensesStatus.VALID);

        final ImmutableSet<String> licenseSignatures = masterLicenseManager().toSignatures(licenses);
        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);

        // all licenses should be stored in the metaData
        assertThat(licenseSignatures, equalTo(licensesMetaData.getSignatures()));

        // only the latest expiry date license for each feature should be returned by getLicenses()
        final List<ESLicense> getLicenses = licensesManagerService.getLicenses();
        TestUtils.isSame(getLicenses, Arrays.asList(shieldLongLicense, marvelLongLicense));
    }

    @Test
    public void testInvalidLicenseStorage() throws Exception {
        LicensesManagerService licensesManagerService = masterLicensesManagerService();
        ESLicense signedLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));

        // modify content of signed license
        ESLicense tamperedLicense = ESLicense.builder()
                .fromLicenseSpec(signedLicense, signedLicense.signature())
                .expiryDate(signedLicense.expiryDate() + 10 * 24 * 60 * 60 * 1000l)
                .verify()
                .build();

        registerAndAckSignedLicenses(licensesManagerService, Arrays.asList(tamperedLicense), LicensesStatus.INVALID);

        // ensure that the invalid license never made it to cluster state
        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        if (licensesMetaData != null) {
            assertThat(licensesMetaData.getSignatures().size(), equalTo(0));
        }
    }

    @Test
    public void testRemoveLicenses() throws Exception {
        LicensesManagerService licensesManagerService = masterLicensesManagerService();

        // generate a trial license for one feature
        final LicensesClientService clientService = licensesClientService();
        final TestTrackingClientListener clientListener = new TestTrackingClientListener(false);
        registerWithTrialLicense(clientService, clientListener, "shield", TimeValue.timeValueHours(1)).run();

        // generate signed licenses for multiple features
        ESLicense shieldShortLicense = generateSignedLicense("shield", TimeValue.timeValueHours(1));
        ESLicense shieldLongLicense = generateSignedLicense("shield", TimeValue.timeValueHours(2));
        ESLicense marvelShortLicense = generateSignedLicense("marvel", TimeValue.timeValueHours(1));
        ESLicense marvelLongLicense = generateSignedLicense("marvel", TimeValue.timeValueHours(2));

        List<ESLicense> licenses = Arrays.asList(shieldLongLicense, shieldShortLicense, marvelLongLicense, marvelShortLicense);
        Collections.shuffle(licenses);
        registerAndAckSignedLicenses(licensesManagerService, licenses, LicensesStatus.VALID);

        // remove license(s) for one feature out of two
        removeAndAckSignedLicenses(licensesManagerService, Sets.newHashSet("shield"));
        final ImmutableSet<String> licenseSignatures = masterLicenseManager().toSignatures(Arrays.asList(marvelLongLicense, marvelShortLicense));
        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licenseSignatures, equalTo(licensesMetaData.getSignatures()));
        // check that trial license is not removed
        assertThat(licensesMetaData.getEncodedTrialLicenses().size(), equalTo(1));

        // remove license(s) for all features
        removeAndAckSignedLicenses(licensesManagerService, Sets.newHashSet("shield", "marvel"));
        licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        assertThat(licensesMetaData.getSignatures().size(), equalTo(0));
        // check that trial license is not removed
        assertThat(licensesMetaData.getEncodedTrialLicenses().size(), equalTo(1));
    }

    private void removeAndAckSignedLicenses(final LicensesManagerService masterLicensesManagerService, final Set<String> featuresToDelete) {
        DeleteLicenseRequest deleteLicenseRequest = new DeleteLicenseRequest(featuresToDelete.toArray(new String[featuresToDelete.size()]));
        LicensesService.DeleteLicenseRequestHolder requestHolder = new LicensesService.DeleteLicenseRequestHolder(deleteLicenseRequest, "test");
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(false);
        masterLicensesManagerService.removeLicenses(requestHolder, new ActionListener<ClusterStateUpdateResponse>() {
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

    private ESLicenseManager masterLicenseManager() {
        InternalTestCluster clients = internalCluster();
        return clients.getInstance(ESLicenseManager.class, clients.getMasterName());
    }

}
