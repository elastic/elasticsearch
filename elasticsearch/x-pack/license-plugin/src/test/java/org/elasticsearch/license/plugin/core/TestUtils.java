/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.licensor.LicenseSigner;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.junit.Assert;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ESTestCase.assertNotNull;
import static org.elasticsearch.test.ESTestCase.awaitBusy;
import static org.elasticsearch.test.ESTestCase.randomAsciiOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class TestUtils {

    private static final FormatDateTimeFormatter formatDateTimeFormatter = Joda.forPattern("yyyy-MM-dd");
    private static final DateMathParser dateMathParser = new DateMathParser(formatDateTimeFormatter);

    public static long dateMath(String time, final long now) {
        return dateMathParser.parse(time, new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return now;
            }
        });
    }
    public static Path getTestPriKeyPath() throws Exception {
        return getResourcePath("/private.key");
    }

    public static Path getTestPubKeyPath() throws Exception {
        return getResourcePath("/public.key");
    }

    public static String dumpLicense(License license) throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        builder.startObject("license");
        license.toInnerXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        return builder.string();
    }

    public static License generateSignedLicense(TimeValue expiryDuration) throws Exception {
        return generateSignedLicense(null, -1, expiryDuration);
    }

    public static License generateSignedLicense(String type, TimeValue expiryDuration) throws Exception {
        return generateSignedLicense(type, -1, expiryDuration);
    }

    public static License generateSignedLicense(long issueDate, TimeValue expiryDuration) throws Exception {
        return generateSignedLicense(null, issueDate, expiryDuration);
    }

    public static License generateSignedLicense(String type, long issueDate, TimeValue expiryDuration) throws Exception {
        return generateSignedLicense(type, randomIntBetween(License.VERSION_START, License.VERSION_CURRENT), issueDate, expiryDuration);
    }

    public static License generateSignedLicense(String type, int version, long issueDate, TimeValue expiryDuration) throws Exception {
        long issue = (issueDate != -1L) ? issueDate : System.currentTimeMillis() - TimeValue.timeValueHours(2).getMillis();
        final String licenseType;
        if (version < License.VERSION_NO_FEATURE_TYPE) {
            licenseType = randomFrom("subscription", "internal", "development");
        } else {
            licenseType = (type != null) ? type : randomFrom("basic", "silver", "dev", "gold", "platinum");
        }
        final License.Builder builder = License.builder()
                .uid(UUID.randomUUID().toString())
                .version(version)
                .expiryDate(System.currentTimeMillis() + expiryDuration.getMillis())
                .issueDate(issue)
                .type(licenseType)
                .issuedTo("customer")
                .issuer("elasticsearch")
                .maxNodes(5);
        if (version == License.VERSION_START) {
            builder.subscriptionType((type != null) ? type : randomFrom("dev", "gold", "platinum", "silver"));
            builder.feature(randomAsciiOfLength(10));
        }
        LicenseSigner signer = new LicenseSigner(getTestPriKeyPath(), getTestPubKeyPath());
        return signer.sign(builder.build());
    }

    public static License generateExpiredLicense(long expiryDate) throws Exception {
        return generateExpiredLicense(randomFrom("basic", "silver", "dev", "gold", "platinum"), expiryDate);
    }

    public static License generateExpiredLicense() throws Exception {
        return generateExpiredLicense(randomFrom("basic", "silver", "dev", "gold", "platinum"));
    }

    public static License generateExpiredLicense(String type) throws Exception {
        return generateExpiredLicense(type,
                System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(1, 10)).getMillis());
    }

    public static License generateExpiredLicense(String type, long expiryDate) throws Exception {
        final License.Builder builder = License.builder()
                .uid(UUID.randomUUID().toString())
                .version(License.VERSION_CURRENT)
                .expiryDate(expiryDate)
                .issueDate(expiryDate - TimeValue.timeValueMinutes(10).getMillis())
                .type(type)
                .issuedTo("customer")
                .issuer("elasticsearch")
                .maxNodes(5);
        LicenseSigner signer = new LicenseSigner(getTestPriKeyPath(), getTestPubKeyPath());
        return signer.sign(builder.build());
    }

    private static Path getResourcePath(String resource) throws Exception {
        return PathUtils.get(TestUtils.class.getResource(resource).toURI());
    }

    public static void registerAndAckSignedLicenses(final LicenseService licenseService, License license,
                                                    final LicensesStatus expectedStatus) {
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(license).acknowledge(true);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<LicensesStatus> status = new AtomicReference<>();
        licenseService.registerLicense(putLicenseRequest, new ActionListener<PutLicenseResponse>() {
            @Override
            public void onResponse(PutLicenseResponse licensesUpdateResponse) {
                status.set(licensesUpdateResponse.status());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
        assertThat(status.get(), equalTo(expectedStatus));
    }

    public static class AssertingLicenseState extends XPackLicenseState {
        public final List<License.OperationMode> modeUpdates = new ArrayList<>();
        public final List<Boolean> activeUpdates = new ArrayList<>();

        @Override
        void update(License.OperationMode mode, boolean active) {
            modeUpdates.add(mode);
            activeUpdates.add(active);
        }
    }
}
