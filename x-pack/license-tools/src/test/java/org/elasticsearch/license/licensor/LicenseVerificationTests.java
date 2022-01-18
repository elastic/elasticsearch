/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license.licensor;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.CryptUtils;
import org.elasticsearch.license.DateUtils;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseVerifier;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PublicKey;

public class LicenseVerificationTests extends ESTestCase {

    protected Path pubKeyPath = null;
    protected PublicKey publicKey;
    protected Path priKeyPath = null;

    @Before
    public void setup() throws Exception {
        pubKeyPath = getDataPath("/public.key");
        publicKey = CryptUtils.readPublicKey(Files.readAllBytes(pubKeyPath));
        priKeyPath = getDataPath("/private.key");
    }

    @After
    public void cleanUp() {
        pubKeyPath = null;
        publicKey = null;
        priKeyPath = null;
    }

    public void testGeneratedLicenses() throws Exception {
        final TimeValue fortyEightHours = TimeValue.timeValueHours(2 * 24);
        final License license = TestUtils.generateSignedLicense(fortyEightHours, pubKeyPath, priKeyPath);
        assertTrue(LicenseVerifier.verifyLicense(license, publicKey));
    }

    public void testLicenseTampering() throws Exception {
        final TimeValue twoHours = TimeValue.timeValueHours(2);
        License license = TestUtils.generateSignedLicense(twoHours, pubKeyPath, priKeyPath);

        final License tamperedLicense = License.builder()
            .fromLicenseSpec(license, license.signature())
            .expiryDate(license.expiryDate() + 10 * 24 * 60 * 60 * 1000L)
            .validate()
            .build();

        assertFalse(LicenseVerifier.verifyLicense(tamperedLicense, publicKey));
    }

    public void testRandomLicenseVerification() throws Exception {
        TestUtils.LicenseSpec licenseSpec = TestUtils.generateRandomLicenseSpec(
            randomIntBetween(License.VERSION_START, License.VERSION_CURRENT)
        );
        License generatedLicense = generateSignedLicense(licenseSpec, pubKeyPath, priKeyPath);
        assertTrue(LicenseVerifier.verifyLicense(generatedLicense, publicKey));
    }

    private static License generateSignedLicense(TestUtils.LicenseSpec spec, Path pubKeyPath, Path priKeyPath) throws Exception {
        LicenseSigner signer = new LicenseSigner(priKeyPath, pubKeyPath);
        License.Builder builder = License.builder()
            .uid(spec.uid)
            .feature(spec.feature)
            .type(spec.type)
            .subscriptionType(spec.subscriptionType)
            .issuedTo(spec.issuedTo)
            .issuer(spec.issuer)
            .maxNodes(spec.maxNodes);

        if (spec.expiryDate != null) {
            builder.expiryDate(DateUtils.endOfTheDay(spec.expiryDate));
        } else {
            builder.expiryDate(spec.expiryDateInMillis);
        }
        if (spec.issueDate != null) {
            builder.issueDate(DateUtils.beginningOfTheDay(spec.issueDate));
        } else {
            builder.issueDate(spec.issueDateInMillis);
        }
        builder.version(spec.version);
        return signer.sign(builder.build());
    }

}
