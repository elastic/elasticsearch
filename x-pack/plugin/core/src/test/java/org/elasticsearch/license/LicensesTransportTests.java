/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.license.LicensesStatus;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

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
//        newSettings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
//        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        newSettings.put(dataNode());
        return newSettings.build();
    }

    public void testEmptyGetLicense() throws Exception {
        // basic license is added async, we should wait for it
        assertBusy(() -> {
            try {
                final ActionFuture<GetLicenseResponse> getLicenseFuture =
                        new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).execute();
                final GetLicenseResponse getLicenseResponse;
                getLicenseResponse = getLicenseFuture.get();
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
        PutLicenseRequestBuilder putLicenseRequestBuilder =
                new PutLicenseRequestBuilder(client().admin().cluster(), PutLicenseAction.INSTANCE).setLicense(signedLicense)
                        .setAcknowledge(true);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get and check license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).get();
        assertThat(getLicenseResponse.license(), equalTo(signedLicense));
    }

    public void testPutLicenseFromString() throws Exception {
        License signedLicense = generateSignedLicense(TimeValue.timeValueMinutes(2));
        String licenseString = TestUtils.dumpLicense(signedLicense);

        // put license source
        PutLicenseRequestBuilder putLicenseRequestBuilder =
                new PutLicenseRequestBuilder(client().admin().cluster(), PutLicenseAction.INSTANCE)
                        .setLicense(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)), XContentType.JSON)
                        .setAcknowledge(true);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get and check license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).get();
        assertThat(getLicenseResponse.license(), equalTo(signedLicense));
    }

    public void testPutInvalidLicense() throws Exception {
        License signedLicense = generateSignedLicense(TimeValue.timeValueMinutes(2));

        // modify content of signed license
        License tamperedLicense = License.builder()
                .fromLicenseSpec(signedLicense, signedLicense.signature())
                .expiryDate(signedLicense.expiryDate() + 10 * 24 * 60 * 60 * 1000L)
                .validate()
                .build();

        PutLicenseRequestBuilder builder = new PutLicenseRequestBuilder(client().admin().cluster(), PutLicenseAction.INSTANCE);
        builder.setLicense(tamperedLicense);

        // try to put license (should be invalid)
        final PutLicenseResponse putLicenseResponse = builder.get();
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.INVALID));

        // try to get invalid license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).get();
        assertThat(getLicenseResponse.license(), not(tamperedLicense));
    }

    public void testPutBasicLicenseIsInvalid() throws Exception {
        License signedLicense = generateSignedLicense("basic", License.VERSION_CURRENT, -1, TimeValue.timeValueMinutes(2));

        PutLicenseRequestBuilder builder = new PutLicenseRequestBuilder(client().admin().cluster(), PutLicenseAction.INSTANCE);
        builder.setLicense(signedLicense);

        // try to put license (should be invalid)
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, builder::get);
        assertEquals(iae.getMessage(), "Registering basic licenses is not allowed.");

        // try to get invalid license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).get();
        assertThat(getLicenseResponse.license(), not(signedLicense));
    }

    public void testPutExpiredLicense() throws Exception {
        License expiredLicense = generateExpiredNonBasicLicense();
        PutLicenseRequestBuilder builder = new PutLicenseRequestBuilder(client().admin().cluster(), PutLicenseAction.INSTANCE);
        builder.setLicense(expiredLicense);
        PutLicenseResponse putLicenseResponse = builder.get();
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.EXPIRED));
        // get license should not return the expired license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).get();
        assertThat(getLicenseResponse.license(), not(expiredLicense));
    }

    public void testPutLicensesSimple() throws Exception {
        License goldSignedLicense = generateSignedLicense("gold", TimeValue.timeValueMinutes(5));
        PutLicenseRequestBuilder putLicenseRequestBuilder =
                new PutLicenseRequestBuilder(client().admin().cluster(), PutLicenseAction.INSTANCE).setLicense(goldSignedLicense)
                        .setAcknowledge(true);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).get();
        assertThat(getLicenseResponse.license(), equalTo(goldSignedLicense));

        License platinumSignedLicense = generateSignedLicense("platinum", TimeValue.timeValueMinutes(2));
        putLicenseRequestBuilder.setLicense(platinumSignedLicense);
        putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));
        getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).get();
        assertThat(getLicenseResponse.license(), equalTo(platinumSignedLicense));
    }

    public void testRemoveLicensesSimple() throws Exception {
        License goldLicense = generateSignedLicense("gold", TimeValue.timeValueMinutes(5));
        PutLicenseRequestBuilder putLicenseRequestBuilder =
                new PutLicenseRequestBuilder(client().admin().cluster(), PutLicenseAction.INSTANCE).setLicense(goldLicense)
                        .setAcknowledge(true);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).get();
        assertThat(getLicenseResponse.license(), equalTo(goldLicense));
        // delete all licenses
        DeleteLicenseRequestBuilder deleteLicenseRequestBuilder =
                new DeleteLicenseRequestBuilder(client().admin().cluster(), DeleteLicenseAction.INSTANCE);
        AcknowledgedResponse deleteLicenseResponse = deleteLicenseRequestBuilder.get();
        assertThat(deleteLicenseResponse.isAcknowledged(), equalTo(true));
        // get licenses (expected no licenses)
        getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster(), GetLicenseAction.INSTANCE).get();
        assertTrue(License.LicenseType.isBasic(getLicenseResponse.license().type()));
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

        PutLicenseRequestBuilder putLicenseRequestBuilder =
                new PutLicenseRequestBuilder(client().admin().cluster(), PutLicenseAction.INSTANCE).setLicense(license)
                        .setAcknowledge(true);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
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

        PutLicenseRequestBuilder putLicenseRequestBuilder =
                new PutLicenseRequestBuilder(client().admin().cluster(), PutLicenseAction.INSTANCE).setLicense(license)
                        .setAcknowledge(true);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));
    }
}
