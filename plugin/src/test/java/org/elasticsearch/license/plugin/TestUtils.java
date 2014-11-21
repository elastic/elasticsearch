/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;
import org.elasticsearch.license.licensor.LicenseSigner;

import java.net.URL;
import java.util.*;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class TestUtils {

    public static String getTestPriKeyPath() throws Exception {
        return getResourcePath("/private.key");
    }

    public static String getTestPubKeyPath() throws Exception {
        return getResourcePath("/public.key");
    }

    public static void isSame(Collection<License> firstLicenses, Collection<License> secondLicenses) {
        isSame(new HashSet<>(firstLicenses), new HashSet<>(secondLicenses));
    }

    public static void isSame(Set<License> firstLicenses, Set<License> secondLicenses) {

        // we do the verifyAndBuild to make sure we weed out any expired licenses
        final Map<String, License> licenses1 = Licenses.reduceAndMap(firstLicenses);
        final Map<String, License> licenses2 = Licenses.reduceAndMap(secondLicenses);

        // check if the effective licenses have the same feature set
        assertThat(licenses1.size(), equalTo(licenses2.size()));

        // for every feature license, check if all the attributes are the same
        for (String featureType : licenses1.keySet()) {
            License license1 = licenses1.get(featureType);
            License license2 = licenses2.get(featureType);
            isSame(license1, license2);
        }
    }

    public static void isSame(License license1, License license2) {
        assertThat(license1.uid(), equalTo(license2.uid()));
        assertThat(license1.feature(), equalTo(license2.feature()));
        assertThat(license1.subscriptionType(), equalTo(license2.subscriptionType()));
        assertThat(license1.type(), equalTo(license2.type()));
        assertThat(license1.issuedTo(), equalTo(license2.issuedTo()));
        assertThat(license1.signature(), equalTo(license2.signature()));
        assertThat(license1.expiryDate(), equalTo(license2.expiryDate()));
        assertThat(license1.issueDate(), equalTo(license2.issueDate()));
        assertThat(license1.maxNodes(), equalTo(license2.maxNodes()));
    }

    public static String dumpLicense(License license) throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        Licenses.toXContent(Collections.singletonList(license), builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        return builder.string();
    }

    public static License generateSignedLicense(String feature, TimeValue expiryDuration) throws Exception {
        return generateSignedLicense(feature, -1, expiryDuration);
    }

    public static License generateSignedLicense(String feature, long issueDate, TimeValue expiryDuration) throws Exception {
        long issue = (issueDate != -1l) ? issueDate : System.currentTimeMillis();
        final License licenseSpec = License.builder()
                .uid(UUID.randomUUID().toString())
                .feature(feature)
                .expiryDate(issue + expiryDuration.getMillis())
                .issueDate(issue)
                .type("subscription")
                .subscriptionType("gold")
                .issuedTo("customer")
                .issuer("elasticsearch")
                .maxNodes(5)
                .build();

        LicenseSigner signer = new LicenseSigner(getTestPriKeyPath(), getTestPubKeyPath());
        return signer.sign(licenseSpec);
    }

    private static String getResourcePath(String resource) throws Exception {
        URL url = TestUtils.class.getResource(resource);
        return url.toURI().getPath();
    }
}
