/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.hamcrest.MatcherAssert;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.hamcrest.core.IsEqual.equalTo;

public class TestUtils {

    private final static FormatDateTimeFormatter formatDateTimeFormatter = Joda.forPattern("yyyy-MM-dd");
    private final static DateMathParser dateMathParser = new DateMathParser(formatDateTimeFormatter);
    private final static DateTimeFormatter dateTimeFormatter = formatDateTimeFormatter.printer();

    public static String dateMathString(String time, final long now) {
        return dateTimeFormatter.print(dateMathParser.parse(time, new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return now;
            }
        }));
    }

    public static long dateMath(String time, final long now) {
        return dateMathParser.parse(time, new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return now;
            }
        });
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
        int maxNodes = randomIntBetween(5, 100);
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
        return licenses.string();
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
}
