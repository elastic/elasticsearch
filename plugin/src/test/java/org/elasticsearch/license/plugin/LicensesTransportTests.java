/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseResponse;
import org.elasticsearch.license.plugin.action.get.GetLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.get.GetLicenseResponse;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

@ClusterScope(scope = TEST, numDataNodes = 10)
public class LicensesTransportTests extends AbstractLicensesIntegrationTests {

    @After
    public void beforeTest() throws Exception {
        wipeAllLicenses();
    }

    @Test
    public void testEmptyGetLicense() throws Exception {
        final ActionFuture<GetLicenseResponse> getLicenseFuture = new GetLicenseRequestBuilder(client().admin().cluster()).execute();
        final GetLicenseResponse getLicenseResponse = getLicenseFuture.get();
        assertThat("expected 0 licenses; but got: " + getLicenseResponse.licenses().size(), getLicenseResponse.licenses().size(), equalTo(0));
    }

    @Test
    public void testPutLicense() throws Exception {
        License signedLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));
        List<License> actualLicenses = Collections.singletonList(signedLicense);

        // put license
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(client().admin().cluster())
                .setLicense(actualLicenses);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses(), notNullValue());
        assertThat(getLicenseResponse.licenses().size(), equalTo(1));

        // check license
        TestUtils.isSame(signedLicense, getLicenseResponse.licenses().get(0));
    }

    @Test
    public void testPutLicenseFromString() throws Exception {
        License signedLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));
        String licenseString = TestUtils.dumpLicense(signedLicense);

        // put license source
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(client().admin().cluster())
                .setLicense(licenseString);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses(), notNullValue());
        assertThat(getLicenseResponse.licenses().size(), equalTo(1));

        // check license
        TestUtils.isSame(signedLicense, getLicenseResponse.licenses().get(0));
    }

    @Test
    public void testPutInvalidLicense() throws Exception {
        License signedLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));

        // modify content of signed license
        License tamperedLicense = License.builder()
                .fromLicenseSpec(signedLicense, signedLicense.signature())
                .expiryDate(signedLicense.expiryDate() + 10 * 24 * 60 * 60 * 1000l)
                .validate()
                .build();

        PutLicenseRequestBuilder builder = new PutLicenseRequestBuilder(client().admin().cluster());
        builder.setLicense(Collections.singletonList(tamperedLicense));

        // try to put license (should be invalid)
        final PutLicenseResponse putLicenseResponse = builder.get();
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.INVALID));

        // try to get invalid license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses().size(), equalTo(0));
    }

    @Test
    public void testPutExpiredLicense() throws Exception {
        License expiredLicense = generateSignedLicense("feature", dateMath("now-10d/d", System.currentTimeMillis()), TimeValue.timeValueMinutes(2));
        License signedLicense = generateSignedLicense("feature", TimeValue.timeValueMinutes(2));

        PutLicenseRequestBuilder builder = new PutLicenseRequestBuilder(client().admin().cluster());
        builder.setLicense(Arrays.asList(signedLicense, expiredLicense));

        // put license should return valid (as there is one valid license)
        final PutLicenseResponse putLicenseResponse = builder.get();
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get license should not return the expired license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses().size(), equalTo(1));

        TestUtils.isSame(getLicenseResponse.licenses().get(0), signedLicense);
    }

    @Test
    public void testPutLicensesForSameFeature() throws Exception {
        License shortedSignedLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));
        License longerSignedLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(5));
        List<License> actualLicenses = Arrays.asList(longerSignedLicense, shortedSignedLicense);

        // put license
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(client().admin().cluster())
                .setLicense(actualLicenses);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get should return only one license (with longer expiry date)
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses(), notNullValue());
        assertThat(getLicenseResponse.licenses().size(), equalTo(1));

        // check license
        TestUtils.isSame(longerSignedLicense, getLicenseResponse.licenses().get(0));
    }

    @Test
    public void testPutLicensesForMultipleFeatures() throws Exception {
        License shieldLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));
        License marvelLicense = generateSignedLicense("marvel", TimeValue.timeValueMinutes(5));
        List<License> actualLicenses = Arrays.asList(marvelLicense, shieldLicense);

        // put license
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(client().admin().cluster())
                .setLicense(actualLicenses);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get should return both the licenses
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses(), notNullValue());

        // check license
        TestUtils.isSame(actualLicenses, getLicenseResponse.licenses());
    }

    @Test
    public void testPutMultipleLicensesForMultipleFeatures() throws Exception {
        License shortedSignedLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));
        License longerSignedLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(5));
        License marvelLicense = generateSignedLicense("marvel", TimeValue.timeValueMinutes(5));
        List<License> actualLicenses = Arrays.asList(marvelLicense, shortedSignedLicense, longerSignedLicense);

        // put license
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(client().admin().cluster())
                .setLicense(actualLicenses);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get should return both the licenses
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses(), notNullValue());
        assertThat(getLicenseResponse.licenses().size(), equalTo(2));

        // check license (should get the longest expiry time for all unique features)
        TestUtils.isSame(Arrays.asList(marvelLicense, longerSignedLicense), getLicenseResponse.licenses());
    }

    @Test
    public void testRemoveLicenseSimple() throws Exception {
        License shieldLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));
        License marvelLicense = generateSignedLicense("marvel", TimeValue.timeValueMinutes(5));
        List<License> actualLicenses = Arrays.asList(marvelLicense, shieldLicense);

        // put two licenses
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(client().admin().cluster())
                .setLicense(actualLicenses);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // get and check licenses
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses(), notNullValue());
        assertThat(getLicenseResponse.licenses().size(), equalTo(2));

        // delete all licenses
        DeleteLicenseRequestBuilder deleteLicenseRequestBuilder = new DeleteLicenseRequestBuilder(client().admin().cluster())
                .setFeatures(Sets.newHashSet("shield", "marvel"));
        DeleteLicenseResponse deleteLicenseResponse = deleteLicenseRequestBuilder.get();
        assertThat(deleteLicenseResponse.isAcknowledged(), equalTo(true));

        // get licenses (expected no licenses)
        getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses(), notNullValue());
        assertThat(getLicenseResponse.licenses().size(), equalTo(0));
    }

    @Test
    public void testRemoveLicenses() throws Exception {
        License shieldLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));
        License marvelLicense = generateSignedLicense("marvel", TimeValue.timeValueMinutes(5));
        List<License> actualLicenses = Arrays.asList(marvelLicense, shieldLicense);

        // put two licenses
        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(client().admin().cluster())
                .setLicense(actualLicenses);
        PutLicenseResponse putLicenseResponse = putLicenseRequestBuilder.get();
        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));
        assertThat(putLicenseResponse.status(), equalTo(LicensesStatus.VALID));

        // delete one license
        DeleteLicenseRequestBuilder deleteLicenseRequestBuilder = new DeleteLicenseRequestBuilder(client().admin().cluster())
                .setFeatures(Sets.newHashSet("shield"));
        DeleteLicenseResponse deleteLicenseResponse = deleteLicenseRequestBuilder.get();
        assertThat(deleteLicenseResponse.isAcknowledged(), equalTo(true));

        // check other license
        GetLicenseResponse getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses(), notNullValue());
        assertThat(getLicenseResponse.licenses().size(), equalTo(1));

        // delete another license
        deleteLicenseRequestBuilder = new DeleteLicenseRequestBuilder(client().admin().cluster())
                .setFeatures(Sets.newHashSet("marvel"));
        deleteLicenseResponse = deleteLicenseRequestBuilder.get();
        assertThat(deleteLicenseResponse.isAcknowledged(), equalTo(true));

        // check no license
        getLicenseResponse = new GetLicenseRequestBuilder(client().admin().cluster()).get();
        assertThat(getLicenseResponse.licenses(), notNullValue());
        assertThat(getLicenseResponse.licenses().size(), equalTo(0));
    }
}
