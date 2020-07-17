/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.licensor.LicenseSigner;
import org.elasticsearch.protocol.xpack.license.LicensesStatus;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static org.apache.lucene.util.LuceneTestCase.createTempFile;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class TestUtils {

    private static final DateFormatter formatDateTimeFormatter = DateFormatter.forPattern("yyyy-MM-dd");
    private static final DateMathParser dateMathParser = formatDateTimeFormatter.toDateMathParser();

    public static String dateMathString(String time, final long now) {
        return formatDateTimeFormatter.format(dateMathParser.parse(time, () -> now).atZone(ZoneOffset.UTC));
    }

    public static long dateMath(String time, final long now) {
        return dateMathParser.parse(time, () -> now).toEpochMilli();
    }

    public static LicenseSpec generateRandomLicenseSpec(int version) {
        boolean datesInMillis = randomBoolean();
        long now = System.currentTimeMillis();
        String uid = UUID.randomUUID().toString();
        String feature = "feature__" + randomInt();
        String issuer = "issuer__"  + randomInt();
        String issuedTo = "issuedTo__" + randomInt();
        final String type;
        final String subscriptionType;
        if (version < License.VERSION_NO_FEATURE_TYPE) {
            subscriptionType = randomFrom("gold", "silver", "platinum");
            type = "subscription";//randomFrom("subscription", "internal", "development");
        } else {
            subscriptionType = null;
            type = randomFrom("basic", "dev", "gold", "silver", "platinum");
        }
        int maxNodes = RandomizedTest.randomIntBetween(5, 100);
        if (datesInMillis) {
            long issueDateInMillis = dateMath("now", now);
            long expiryDateInMillis = dateMath("now+10d/d", now);
            return new LicenseSpec(version, uid, feature, issueDateInMillis, expiryDateInMillis, type, subscriptionType, issuedTo, issuer,
                maxNodes);
        } else {
            String issueDate = dateMathString("now", now);
            String expiryDate = dateMathString("now+10d/d", now);
            return new LicenseSpec(version, uid, feature, issueDate, expiryDate, type, subscriptionType, issuedTo, issuer, maxNodes);
        }
    }

    public static String generateLicenseSpecString(LicenseSpec licenseSpec) throws IOException {
        XContentBuilder licenses = jsonBuilder();
        licenses.startObject();
        licenses.startArray("licenses");
        licenses.startObject()
            .field("uid", licenseSpec.uid)
            .field("type", licenseSpec.type)
            .field("subscription_type", licenseSpec.subscriptionType)
            .field("issued_to", licenseSpec.issuedTo)
            .field("issuer", licenseSpec.issuer)
            .field("feature", licenseSpec.feature)
            .field("max_nodes", licenseSpec.maxNodes);

        if (licenseSpec.issueDate != null) {
            licenses.field("issue_date", licenseSpec.issueDate);
        } else {
            licenses.field("issue_date_in_millis", licenseSpec.issueDateInMillis);
        }
        if (licenseSpec.expiryDate != null) {
            licenses.field("expiry_date", licenseSpec.expiryDate);
        } else {
            licenses.field("expiry_date_in_millis", licenseSpec.expiryDateInMillis);
        }
        licenses.field("version", licenseSpec.version);
        licenses.endObject();
        licenses.endArray();
        licenses.endObject();
        return Strings.toString(licenses);
    }

    public static License generateLicenses(LicenseSpec spec) {
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
        return builder.build();
    }

    public static void assertLicenseSpec(LicenseSpec spec, License license) {
        MatcherAssert.assertThat(license.uid(), equalTo(spec.uid));
        MatcherAssert.assertThat(license.issuedTo(), equalTo(spec.issuedTo));
        MatcherAssert.assertThat(license.issuer(), equalTo(spec.issuer));
        MatcherAssert.assertThat(license.type(), equalTo(spec.type));
        MatcherAssert.assertThat(license.maxNodes(), equalTo(spec.maxNodes));
        if (spec.issueDate != null) {
            MatcherAssert.assertThat(license.issueDate(), equalTo(DateUtils.beginningOfTheDay(spec.issueDate)));
        } else {
            MatcherAssert.assertThat(license.issueDate(), equalTo(spec.issueDateInMillis));
        }
        if (spec.expiryDate != null) {
            MatcherAssert.assertThat(license.expiryDate(), equalTo(DateUtils.endOfTheDay(spec.expiryDate)));
        } else {
            MatcherAssert.assertThat(license.expiryDate(), equalTo(spec.expiryDateInMillis));
        }
    }

    public static class LicenseSpec {
        public final int version;
        public final String feature;
        public final String issueDate;
        public final long issueDateInMillis;
        public final String expiryDate;
        public final long expiryDateInMillis;
        public final String uid;
        public final String type;
        public final String subscriptionType;
        public final String issuedTo;
        public final String issuer;
        public final int maxNodes;

        public LicenseSpec(String issueDate, String expiryDate) {
            this(License.VERSION_CURRENT, UUID.randomUUID().toString(), "feature", issueDate, expiryDate, "trial", "none", "customer",
                "elasticsearch", 5);
        }

        public LicenseSpec(int version, String uid, String feature, long issueDateInMillis, long expiryDateInMillis, String type,
                           String subscriptionType, String issuedTo, String issuer, int maxNodes) {
            this.version = version;
            this.feature = feature;
            this.issueDateInMillis = issueDateInMillis;
            this.issueDate = null;
            this.expiryDateInMillis = expiryDateInMillis;
            this.expiryDate = null;
            this.uid = uid;
            this.type = type;
            this.subscriptionType = subscriptionType;
            this.issuedTo = issuedTo;
            this.issuer = issuer;
            this.maxNodes = maxNodes;
        }

        public LicenseSpec(int version, String uid, String feature, String issueDate, String expiryDate, String type,
                           String subscriptionType, String issuedTo, String issuer, int maxNodes) {
            this.version = version;
            this.feature = feature;
            this.issueDate = issueDate;
            this.issueDateInMillis = -1;
            this.expiryDate = expiryDate;
            this.expiryDateInMillis = -1;
            this.uid = uid;
            this.type = type;
            this.subscriptionType = subscriptionType;
            this.issuedTo = issuedTo;
            this.issuer = issuer;
            this.maxNodes = maxNodes;
        }
    }
    private static Path getTestPriKeyPath() throws Exception {
        return getResourcePath("/private.key");
    }

    private static Path getTestPubKeyPath() throws Exception {
        return getResourcePath("/public.key");
    }

    public static String dumpLicense(License license) throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        builder.startObject("license");
        license.toInnerXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        return Strings.toString(builder);
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

    public static License generateSignedLicenseOldSignature() {
        long issueDate = System.currentTimeMillis();
        License.Builder specBuilder = License.builder()
                .uid(UUID.randomUUID().toString())
                .version(License.VERSION_START_DATE)
                .issuedTo("customer")
                .maxNodes(5)
                .type("trial")
                .issueDate(issueDate)
                .expiryDate(issueDate + TimeValue.timeValueHours(24).getMillis());
        return SelfGeneratedLicense.create(specBuilder, License.VERSION_START_DATE);
    }

    /**
     * This method which chooses the license type randomly if the type is null. However, it will not randomly
     * choose trial or basic types as those types can only be self-generated.
     */
    public static License generateSignedLicense(String type, int version, long issueDate, TimeValue expiryDuration) throws Exception {
        long issue = (issueDate != -1L) ? issueDate : System.currentTimeMillis() - TimeValue.timeValueHours(2).getMillis();
        final String licenseType;
        if (version < License.VERSION_NO_FEATURE_TYPE) {
            licenseType = randomFrom("subscription", "internal", "development");
        } else {
            licenseType = (type != null) ? type : randomFrom("silver", "dev", "gold", "platinum");
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
            builder.feature(randomAlphaOfLength(10));
        }
        if ("enterprise".equals(licenseType)) {
            builder.version(License.VERSION_ENTERPRISE)
                .maxResourceUnits(randomIntBetween(5, 500))
                .maxNodes(-1);
        }
        final LicenseSigner signer = new LicenseSigner(getTestPriKeyPath(), getTestPubKeyPath());
        return signer.sign(builder.build());
    }

    public static License generateSignedLicense(License.Builder builder) throws Exception {
        LicenseSigner signer = new LicenseSigner(getTestPriKeyPath(), getTestPubKeyPath());
        return signer.sign(builder.build());
    }

    public static License generateExpiredNonBasicLicense(long expiryDate) throws Exception {
        return generateExpiredNonBasicLicense(randomFrom("silver", "dev", "gold", "platinum"), expiryDate);
    }

    public static License generateExpiredNonBasicLicense() throws Exception {
        return generateExpiredNonBasicLicense(randomFrom("silver", "dev", "gold", "platinum"));
    }

    public static License generateExpiredNonBasicLicense(String type) throws Exception {
        return generateExpiredNonBasicLicense(type,
                System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(1, 10)).getMillis());
    }

    public static License generateExpiredNonBasicLicense(String type, long expiryDate) throws Exception {
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
        Path resourceFile = createTempFile();
        try (InputStream resourceInput = TestUtils.class.getResourceAsStream(resource)) {
            Files.copy(resourceInput, resourceFile, StandardCopyOption.REPLACE_EXISTING);
        }
        return resourceFile;
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
        public final List<Version> trialVersionUpdates = new ArrayList<>();

        public AssertingLicenseState() {
            super(Settings.EMPTY, () -> 0);
        }

        @Override
        void update(License.OperationMode mode, boolean active, Version mostRecentTrialVersion) {
            modeUpdates.add(mode);
            activeUpdates.add(active);
            trialVersionUpdates.add(mostRecentTrialVersion);
        }
    }

    /**
     * A license state that makes the {@link #update(License.OperationMode, boolean, Version)}
     * method public for use in tests.
     */
    public static class UpdatableLicenseState extends XPackLicenseState {
        public UpdatableLicenseState() {
            this(Settings.EMPTY);
        }

        public UpdatableLicenseState(Settings settings) {
            super(settings, () -> 0);
        }

        @Override
        public void update(License.OperationMode mode, boolean active, Version mostRecentTrialVersion) {
            super.update(mode, active, mostRecentTrialVersion);
        }
    }

    public static XPackLicenseState newTestLicenseState() {
        return new XPackLicenseState(Settings.EMPTY, () -> 0);
    }

    public static void putLicense(Metadata.Builder builder, License license) {
        builder.putCustom(LicensesMetadata.TYPE, new LicensesMetadata(license, null));
    }
}
