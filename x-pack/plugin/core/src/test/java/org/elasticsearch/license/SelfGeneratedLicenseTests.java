/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.UUID;

import static org.elasticsearch.license.CryptUtils.encryptV3Format;
import static org.hamcrest.Matchers.equalTo;


public class SelfGeneratedLicenseTests extends ESTestCase {

    public void testBasic() throws Exception {
        long issueDate = System.currentTimeMillis();
        License.Builder specBuilder = License.builder()
                .uid(UUID.randomUUID().toString())
                .issuedTo("customer")
                .maxNodes(5)
                .type(randomBoolean() ? "trial" : "basic")
                .issueDate(issueDate)
                .expiryDate(issueDate + TimeValue.timeValueHours(2).getMillis());
        License trialLicense = SelfGeneratedLicense.create(specBuilder, License.VERSION_CURRENT);
        assertThat(SelfGeneratedLicense.verify(trialLicense), equalTo(true));
    }

    public void testTampered() throws Exception {
        long issueDate = System.currentTimeMillis();
        License.Builder specBuilder = License.builder()
                .uid(UUID.randomUUID().toString())
                .issuedTo("customer")
                .type(randomBoolean() ? "trial" : "basic")
                .maxNodes(5)
                .issueDate(issueDate)
                .expiryDate(issueDate + TimeValue.timeValueHours(2).getMillis());
        License trialLicense = SelfGeneratedLicense.create(specBuilder, License.VERSION_CURRENT);
        final String originalSignature = trialLicense.signature();
        License tamperedLicense = License.builder().fromLicenseSpec(trialLicense, originalSignature)
                .expiryDate(System.currentTimeMillis() + TimeValue.timeValueHours(5).getMillis())
                .build();
        assertThat(SelfGeneratedLicense.verify(trialLicense), equalTo(true));
        assertThat(SelfGeneratedLicense.verify(tamperedLicense), equalTo(false));
    }

    public void testFrom1x() throws Exception {
        long issueDate = System.currentTimeMillis();
        License.Builder specBuilder = License.builder()
                .uid(UUID.randomUUID().toString())
                .issuedTo("customer")
                .type("subscription")
                .subscriptionType("trial")
                .issuer("elasticsearch")
                .feature("")
                .version(License.VERSION_START)
                .maxNodes(5)
                .issueDate(issueDate)
                .expiryDate(issueDate + TimeValue.timeValueHours(2).getMillis());
        License pre20TrialLicense = specBuilder.build();
        License license = SelfGeneratedLicense.create(License.builder().fromPre20LicenseSpec(pre20TrialLicense).type("trial"),
            License.VERSION_CURRENT);
        assertThat(SelfGeneratedLicense.verify(license), equalTo(true));
    }

    public void testTrialLicenseVerifyWithOlderVersion() throws Exception {
        assumeFalse("Can't run in a FIPS JVM. We can't generate old licenses since PBEWithSHA1AndDESede is not available", inFipsJvm());
        long issueDate = System.currentTimeMillis();
        License.Builder specBuilder = License.builder()
                .issuedTo("customer")
                .maxNodes(5)
                .issueDate(issueDate)
                .expiryDate(issueDate + TimeValue.timeValueHours(2).getMillis())
                .feature("")
                .subscriptionType("trial")
                .version(1);
        License trialLicenseV1 = createTrialLicense(specBuilder);
        assertThat(SelfGeneratedLicense.verify(trialLicenseV1), equalTo(true));
    }

    private static License createTrialLicense(License.Builder specBuilder) {
        License spec = specBuilder
                .type(randomBoolean() ? "trial" : "basic")
                .issuer("elasticsearch")
                .uid(UUID.randomUUID().toString())
                .build();
        final String signature;
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            spec.toXContent(contentBuilder, new ToXContent.MapParams(Collections.singletonMap(License.LICENSE_SPEC_VIEW_MODE, "true")));
            byte[] encrypt = encryptV3Format(BytesReference.toBytes(BytesReference.bytes(contentBuilder)));
            byte[] bytes = new byte[4 + 4 + encrypt.length];
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.putInt(-spec.version())
                    .putInt(encrypt.length)
                    .put(encrypt);
            signature = Base64.getEncoder().encodeToString(bytes);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return License.builder().fromLicenseSpec(spec, signature).build();
    }
}
