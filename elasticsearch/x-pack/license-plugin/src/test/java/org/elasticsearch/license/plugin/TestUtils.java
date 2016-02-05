/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
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
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.junit.Assert;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
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

    private final static FormatDateTimeFormatter formatDateTimeFormatter = Joda.forPattern("yyyy-MM-dd");
    private final static DateMathParser dateMathParser = new DateMathParser(formatDateTimeFormatter);

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
        long issue = (issueDate != -1L) ? issueDate : System.currentTimeMillis();
        int version = randomIntBetween(License.VERSION_START, License.VERSION_CURRENT);
        final String licenseType;
        if (version < License.VERSION_NO_FEATURE_TYPE) {
            licenseType = randomFrom("subscription", "internal", "development");
        } else {
            licenseType = (type != null) ? type : randomFrom("basic", "silver", "dev", "gold", "platinum");
        }
        final License.Builder builder = License.builder()
                .uid(UUID.randomUUID().toString())
                .version(version)
                .expiryDate(issue + expiryDuration.getMillis())
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

    private static Path getResourcePath(String resource) throws Exception {
        return PathUtils.get(TestUtils.class.getResource(resource).toURI());
    }

    public static void awaitNoBlock(final Client client) throws InterruptedException {
        boolean success = awaitBusy(() -> {
            Set<ClusterBlock> clusterBlocks = client.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                    .getState().blocks().global(ClusterBlockLevel.METADATA_WRITE);
            return clusterBlocks.isEmpty();
        });
        assertThat("awaiting no block for too long", success, equalTo(true));
    }

    public static void awaitNoPendingTasks(final Client client) throws InterruptedException {
        boolean success = awaitBusy(() -> client.admin().cluster().preparePendingClusterTasks().get().getPendingTasks().isEmpty());
        assertThat("awaiting no pending tasks for too long", success, equalTo(true));
    }

    public static void registerAndAckSignedLicenses(final LicensesService licensesService, License license,
                                                    final LicensesStatus expectedStatus) {
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().license(license);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<LicensesStatus> status = new AtomicReference<>();
        licensesService.registerLicense(putLicenseRequest, new ActionListener<LicensesService.LicensesUpdateResponse>() {
            @Override
            public void onResponse(LicensesService.LicensesUpdateResponse licensesUpdateResponse) {
                status.set(licensesUpdateResponse.status());
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
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

    public static class AssertingLicensee implements Licensee {
        public final ESLogger logger;
        public final String id;
        public final List<LicenseState> licenseStates = new CopyOnWriteArrayList<>();
        public final AtomicInteger expirationMessagesCalled = new AtomicInteger(0);
        public final List<Tuple<License, License>> acknowledgementRequested = new CopyOnWriteArrayList<>();

        private String[] acknowledgmentMessages = new String[0];

        public AssertingLicensee(String id, ESLogger logger) {
            this.logger = logger;
            this.id = id;
        }

        public void setAcknowledgementMessages(String[] acknowledgementMessages) {
            this.acknowledgmentMessages = acknowledgementMessages;
        }
        @Override
        public String id() {
            return id;
        }

        @Override
        public String[] expirationMessages() {
            expirationMessagesCalled.incrementAndGet();
            return new String[0];
        }

        @Override
        public String[] acknowledgmentMessages(License currentLicense, License newLicense) {
            acknowledgementRequested.add(new Tuple<>(currentLicense, newLicense));
            return acknowledgmentMessages;
        }

        @Override
        public void onChange(Status status) {
            assertNotNull(status);
            licenseStates.add(status.getLicenseState());
        }
    }
}
