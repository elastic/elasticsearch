/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.core.DateUtils;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.licensor.LicenseSigner;
import org.elasticsearch.license.core.LicenseVerifier;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ElasticsearchTestCase.randomFrom;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith(value = com.carrotsearch.randomizedtesting.RandomizedRunner.class)
public abstract class AbstractLicensingTestBase {

    protected static String pubKeyPath = null;
    protected static String priKeyPath = null;

    private final static FormatDateTimeFormatter formatDateTimeFormatter = Joda.forPattern("yyyy-MM-dd");
    private final static org.elasticsearch.common.joda.time.format.DateTimeFormatter dateTimeFormatter = formatDateTimeFormatter.printer();
    private final static DateMathParser dateMathParser = new DateMathParser(formatDateTimeFormatter, TimeUnit.MILLISECONDS);

    @BeforeClass
    public static void setup() throws Exception {
        pubKeyPath = getResourcePath("/public.key");
        priKeyPath = getResourcePath("/private.key");
    }

    public static String dateMathString(String time, long now) {
        return dateTimeFormatter.print(dateMathParser.parse(time, now));
    }

    public static long dateMath(String time, long now) {
        return dateMathParser.parse(time, now);
    }

    public static String getResourcePath(String resource) throws Exception {
        URL url = LicenseVerifier.class.getResource(resource);
        return url.toURI().getPath();
    }

    public static LicenseSpec generateRandomLicenseSpec() {
        boolean datesInMillis = randomBoolean();
        long now = System.currentTimeMillis();
        String uid = UUID.randomUUID().toString();
        String feature = "feature__" + randomInt();
        String issuer = "issuer__"  + randomInt();
        String issuedTo = "issuedTo__" + randomInt();
        String type = randomFrom("subscription", "internal", "development");
        String subscriptionType = randomFrom("none", "gold", "silver", "platinum");
        int maxNodes = randomIntBetween(5, 100);
        if (datesInMillis) {
            long issueDateInMillis = dateMath("now", now);
            long expiryDateInMillis = dateMath("now+10d/d", now);
            return new LicenseSpec(uid, feature, issueDateInMillis, expiryDateInMillis, type, subscriptionType, issuedTo, issuer, maxNodes);
        } else {
            String issueDate = dateMathString("now", now);
            String expiryDate = dateMathString("now+10d/d", now);
            return new LicenseSpec(uid, feature, issueDate, expiryDate, type, subscriptionType, issuedTo, issuer, maxNodes);
        }
    }

    public static String generateLicenseSpecString(List<LicenseSpec> licenseSpecs) throws IOException {
        XContentBuilder licenses = jsonBuilder();
        licenses.startObject();
        licenses.startArray("licenses");
        for (LicenseSpec licenseSpec : licenseSpecs) {
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
            licenses.endObject();
        }
        licenses.endArray();
        licenses.endObject();
        return licenses.string();
    }

    public static Set<License> generateSignedLicenses(List<LicenseSpec> licenseSpecs) throws Exception {
        LicenseSigner signer = new LicenseSigner(priKeyPath, pubKeyPath);
        Set<License> unSignedLicenses = new HashSet<>();
        for (LicenseSpec spec : licenseSpecs) {
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
            unSignedLicenses.add(builder.build());
        }
        return signer.sign(unSignedLicenses);
    }

    public static class LicenseSpec {
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

        public LicenseSpec(String feature, String issueDate, String expiryDate) {
            this(UUID.randomUUID().toString(), feature, issueDate, expiryDate, "trial", "none", "customer", "elasticsearch", 5);
        }

        public LicenseSpec(String uid, String feature, long issueDateInMillis, long expiryDateInMillis, String type,
                           String subscriptionType, String issuedTo, String issuer, int maxNodes) {
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

        public LicenseSpec(String uid, String feature, String issueDate, String expiryDate, String type,
                           String subscriptionType, String issuedTo, String issuer, int maxNodes) {
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

    public static void assertLicenseSpec(LicenseSpec spec, License license) {
        assertThat(license.uid(), equalTo(spec.uid));
        assertThat(license.feature(), equalTo(spec.feature));
        assertThat(license.issuedTo(), equalTo(spec.issuedTo));
        assertThat(license.issuer(), equalTo(spec.issuer));
        assertThat(license.type(), equalTo(spec.type));
        assertThat(license.subscriptionType(), equalTo(spec.subscriptionType));
        assertThat(license.maxNodes(), equalTo(spec.maxNodes));
        if (spec.issueDate != null) {
            assertThat(license.issueDate(), equalTo(DateUtils.beginningOfTheDay(spec.issueDate)));
        } else {
            assertThat(license.issueDate(), equalTo(spec.issueDateInMillis));
        }
        if (spec.expiryDate != null) {
            assertThat(license.expiryDate(), equalTo(DateUtils.endOfTheDay(spec.expiryDate)));
        } else {
            assertThat(license.expiryDate(), equalTo(spec.expiryDateInMillis));
        }
    }
}
