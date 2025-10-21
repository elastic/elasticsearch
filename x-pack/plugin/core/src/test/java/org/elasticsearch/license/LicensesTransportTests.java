/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.license.GetLicenseRequest;
import org.elasticsearch.protocol.xpack.license.LicensesStatus;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.UnaryOperator;

import static org.elasticsearch.license.TestUtils.dateMath;
import static org.elasticsearch.license.TestUtils.generateExpiredNonBasicLicense;
import static org.elasticsearch.license.TestUtils.generateSignedLicense;
import static org.elasticsearch.test.NodeRoles.dataNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class LicensesTransportTests extends ESSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        // newSettings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        // newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        newSettings.put(dataNode());
        return newSettings.build();
    }

    public void testEmptyGetLicense() throws Exception {
        // basic license is added async, we should wait for it
        assertBusy(() -> {
            try {
                final GetLicenseResponse getLicenseResponse = getLicense();
                assertNotNull(getLicenseResponse.license());
                assertThat(getLicenseResponse.license().operationMode(), equalTo(License.OperationMode.BASIC));
            } catch (Exception e) {
                throw new RuntimeException("unexpected exception", e);
            }
        });
    }

    public void testPutLicense() throws Exception {
        License signedLicense = generateSignedLicense(TimeValue.timeValueMinutes(2));

        // put license
        final var putLicenseResponse = putLicense(plr -> plr.license(signedLicense).acknowledge(true));
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get and check license
        assertThat(getLicense().license(), equalTo(signedLicense));
    }

    public void testPutLicenseFromString() throws Exception {
        License signedLicense = generateSignedLicense(TimeValue.timeValueMinutes(2));
        String licenseString = TestUtils.dumpLicense(signedLicense);

        // put license source
        final var putLicenseResponse = putLicense(
            plr -> plr.license(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)), XContentType.JSON).acknowledge(true)
        );
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get and check license
        assertThat(getLicense().license(), equalTo(signedLicense));
    }

    public void testPutInvalidLicense() throws Exception {
        License signedLicense = generateSignedLicense(TimeValue.timeValueMinutes(2));

        // modify content of signed license
        License tamperedLicense = License.builder()
            .fromLicenseSpec(signedLicense, signedLicense.signature())
            .expiryDate(signedLicense.expiryDate() + 10 * 24 * 60 * 60 * 1000L)
            .validate()
            .build();
        final var putLicenseResponse = putLicense(plr -> plr.license(tamperedLicense).acknowledge(randomBoolean()));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.INVALID));

        // try to get invalid license
        assertThat(getLicense().license(), not(tamperedLicense));
    }

    public void testPutBasicLicenseIsInvalid() throws Exception {
        License signedLicense = generateSignedLicense("basic", License.VERSION_CURRENT, -1, TimeValue.timeValueMinutes(2));

        // try to put license (should be invalid)
        final var putLicenseFuture = putLicenseFuture(plr -> plr.license(signedLicense).acknowledge(randomBoolean()));
        IllegalArgumentException iae = expectThrows(ExecutionException.class, IllegalArgumentException.class, putLicenseFuture::get);
        assertEquals(iae.getMessage(), "Registering basic licenses is not allowed.");

        // try to get invalid license
        assertThat(getLicense().license(), not(signedLicense));
    }

    public void testPutExpiredLicense() throws Exception {
        License expiredLicense = generateExpiredNonBasicLicense();
        final var putLicenseResponse = putLicense(plr -> plr.license(expiredLicense).acknowledge(randomBoolean()));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.EXPIRED));
        // get license should not return the expired license
        assertThat(getLicense().license(), not(expiredLicense));
    }

    public void testPutLicensesSimple() throws Exception {
        License goldSignedLicense = generateSignedLicense("gold", TimeValue.timeValueMinutes(5));
        final var putGoldLicenseResponse = putLicense(plr -> plr.license(goldSignedLicense).acknowledge(true));
        assertThat(putGoldLicenseResponse.status(), equalTo(LicensesStatus.VALID));
        assertThat(getLicense().license(), equalTo(goldSignedLicense));

        License platinumSignedLicense = generateSignedLicense("platinum", TimeValue.timeValueMinutes(2));
        final var putPlatinumLicenseResponse = putLicense(plr -> plr.license(platinumSignedLicense).acknowledge(true));
        assertThat(putPlatinumLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putPlatinumLicenseResponse.status(), equalTo(LicensesStatus.VALID));
        assertThat(getLicense().license(), equalTo(platinumSignedLicense));
    }

    public void testRemoveLicensesSimple() throws Exception {
        License goldLicense = generateSignedLicense("gold", TimeValue.timeValueMinutes(5));
        PutLicenseResponse putLicenseResponse = putLicense(plr -> plr.license(goldLicense).acknowledge(true));
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));
        assertThat(getLicense().license(), equalTo(goldLicense));
        // delete all licenses
        AcknowledgedResponse deleteLicenseResponse = deleteLicense();
        assertThat(deleteLicenseResponse.isAcknowledged(), equalTo(true));
        // get licenses (expected no licenses)
        assertTrue(License.LicenseType.isBasic(getLicense().license().type()));
    }

    public void testLicenseIsRejectWhenStartDateLaterThanNow() throws Exception {
        long now = System.currentTimeMillis();
        final License.Builder builder = License.builder()
            .uid(UUID.randomUUID().toString())
            .version(License.VERSION_CURRENT)
            .expiryDate(dateMath("now+2h", now))
            .startDate(dateMath("now+1h", now))
            .issueDate(now)
            .type(License.OperationMode.TRIAL.toString())
            .issuedTo("customer")
            .issuer("elasticsearch")
            .maxNodes(5);
        License license = TestUtils.generateSignedLicense(builder);

        PutLicenseResponse putLicenseResponse = putLicense(plr -> plr.license(license).acknowledge(true));
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.INVALID));
    }

    public void testLicenseIsAcceptedWhenStartDateBeforeThanNow() throws Exception {
        long now = System.currentTimeMillis();
        final License.Builder builder = License.builder()
            .uid(UUID.randomUUID().toString())
            .version(License.VERSION_CURRENT)
            .expiryDate(dateMath("now+2h", now))
            .startDate(now)
            .issueDate(now)
            .type(License.OperationMode.TRIAL.toString())
            .issuedTo("customer")
            .issuer("elasticsearch")
            .maxNodes(5);
        License license = TestUtils.generateSignedLicense(builder);

        PutLicenseResponse putLicenseResponse = putLicense(plr -> plr.license(license).acknowledge(true));
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));
    }

    private GetLicenseResponse getLicense() {
        return safeGet(clusterAdmin().execute(GetLicenseAction.INSTANCE, new GetLicenseRequest(TEST_REQUEST_TIMEOUT)));
    }

    private Future<PutLicenseResponse> putLicenseFuture(UnaryOperator<PutLicenseRequest> onRequest) {
        return clusterAdmin().execute(
            PutLicenseAction.INSTANCE,
            onRequest.apply(new PutLicenseRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        );
    }

    private PutLicenseResponse putLicense(UnaryOperator<PutLicenseRequest> onRequest) {
        return safeGet(putLicenseFuture(onRequest));
    }

    private AcknowledgedResponse deleteLicense() {
        return safeGet(
            clusterAdmin().execute(
                TransportDeleteLicenseAction.TYPE,
                new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            )
        );
    }
}
