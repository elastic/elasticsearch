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
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.licensor.ESLicenseSigner;
import org.elasticsearch.license.manager.ESLicenseManager;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomRealisticUnicodeOfCodepointLengthBetween;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ElasticsearchTestCase.randomFrom;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith(value = com.carrotsearch.randomizedtesting.RandomizedRunner.class)
public class AbstractLicensingTestBase {

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

    protected static String dateMathString(String time, long now) {
        return dateTimeFormatter.print(dateMathParser.parse(time, now));
    }

    protected static long dateMath(String time, long now) {
        return dateMathParser.parse(time, now);
    }

    public static String getTestPriKeyPath() throws Exception {
        return getResourcePath("/private.key");
    }

    public static String getTestPubKeyPath() throws Exception {
        return getResourcePath("/public.key");
    }

    public static String getResourcePath(String resource) throws Exception {
        URL url = ESLicenseManager.class.getResource(resource);
        return url.toURI().getPath();
    }


    public static LicenseSpec generateRandomLicenseSpec() {
        long now = System.currentTimeMillis();
        String issueDate = dateMathString("now", now);
        String expiryDate = dateMathString("now+10d/d", now);
        String uid = randomRealisticUnicodeOfCodepointLengthBetween(2, 10);
        String feature = randomRealisticUnicodeOfCodepointLengthBetween(5, 15);
        String issuer = randomRealisticUnicodeOfCodepointLengthBetween(5, 15);
        String issuedTo = randomRealisticUnicodeOfCodepointLengthBetween(5, 15);
        String type = randomFrom("subscription", "internal", "development");
        String subscriptionType = randomFrom("none", "gold", "silver", "platinum");
        int maxNodes = randomIntBetween(5, 100);

        return new LicenseSpec(uid, feature, issueDate, expiryDate, type, subscriptionType, issuedTo, issuer, maxNodes);
    }

    public static String generateESLicenseSpecString(List<LicenseSpec> licenseSpecs) throws IOException {
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
                    .field("issue_date", licenseSpec.issueDate)
                    .field("expiry_date", licenseSpec.expiryDate)
                    .field("feature", licenseSpec.feature)
                    .field("max_nodes", licenseSpec.maxNodes)
                    .endObject();
        }
        licenses.endArray();
        licenses.endObject();
        return licenses.string();
    }

    public static Set<ESLicense> generateSignedLicenses(List<LicenseSpec> licenseSpecs) throws Exception {
        ESLicenseSigner signer = new ESLicenseSigner(getTestPriKeyPath(), getTestPubKeyPath());
        Set<ESLicense> unSignedLicenses = new HashSet<>();
        for (LicenseSpec spec : licenseSpecs) {
            unSignedLicenses.add(ESLicense.builder()
                            .uid(spec.uid)
                            .feature(spec.feature)
                            .expiryDate(DateUtils.endOfTheDay(spec.expiryDate))
                            .issueDate(DateUtils.beginningOfTheDay(spec.issueDate))
                            .type(spec.type)
                            .subscriptionType(spec.subscriptionType)
                            .issuedTo(spec.issuedTo)
                            .issuer(spec.issuer)
                            .maxNodes(spec.maxNodes)
                            .build()
            );
        }
        return signer.sign(unSignedLicenses);
    }

    public static ESLicense generateSignedLicense(String feature, TimeValue expiryDuration) throws Exception {
        return generateSignedLicense(feature, -1, expiryDuration);
    }

    public static ESLicense generateSignedLicense(String feature, long issueDate, TimeValue expiryDuration) throws Exception {
        long issue = (issueDate != -1l) ? issueDate : System.currentTimeMillis();
        final ESLicense licenseSpec = ESLicense.builder()
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

        ESLicenseSigner signer = new ESLicenseSigner(getTestPriKeyPath(), getTestPubKeyPath());
        return signer.sign(licenseSpec);
    }

    public static class LicenseSpec {
        public final String feature;
        public final String issueDate;
        public final String expiryDate;
        public final String uid;
        public final String type;
        public final String subscriptionType;
        public final String issuedTo;
        public final String issuer;
        public final int maxNodes;

        public LicenseSpec(String feature, String issueDate, String expiryDate) {
            this(UUID.randomUUID().toString(), feature, issueDate, expiryDate, "trial", "none", "customer", "elasticsearch", 5);
        }

        public LicenseSpec(String uid, String feature, String issueDate, String expiryDate, String type,
                           String subscriptionType, String issuedTo, String issuer, int maxNodes) {
            this.feature = feature;
            this.issueDate = issueDate;
            this.expiryDate = expiryDate;
            this.uid = uid;
            this.type = type;
            this.subscriptionType = subscriptionType;
            this.issuedTo = issuedTo;
            this.issuer = issuer;
            this.maxNodes = maxNodes;
        }
    }

    public static void assertLicenseSpec(LicenseSpec spec, ESLicense license) {
        assertThat(license.uid(), equalTo(spec.uid));
        assertThat(license.feature(), equalTo(spec.feature));
        assertThat(license.issuedTo(), equalTo(spec.issuedTo));
        assertThat(license.issuer(), equalTo(spec.issuer));
        assertThat(license.type(), equalTo(spec.type));
        assertThat(license.subscriptionType(), equalTo(spec.subscriptionType));
        assertThat(license.maxNodes(), equalTo(spec.maxNodes));
        assertThat(license.issueDate(), equalTo(DateUtils.beginningOfTheDay(spec.issueDate)));
        assertThat(license.expiryDate(), equalTo(DateUtils.endOfTheDay(spec.expiryDate)));
    }
}
