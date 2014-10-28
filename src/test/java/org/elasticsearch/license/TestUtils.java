/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.core.DateUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.licensor.tools.LicenseGeneratorTool;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertTrue;

public class TestUtils {

    public final static String SHIELD = "shield";
    public final static String MARVEL = "marvel";

    public static String generateESLicenses(Map<String, FeatureAttributes> featureAttributes) throws IOException {
        XContentBuilder licenses = jsonBuilder();
        licenses.startObject();
        licenses.startArray("licenses");
        for (FeatureAttributes attributes : featureAttributes.values()) {
            licenses.startObject()
                    .field("uid", attributes.uid)
                    .field("type", attributes.type)
                    .field("subscription_type", attributes.subscriptionType)
                    .field("issued_to", attributes.issuedTo)
                    .field("issuer", attributes.issuer)
                    .field("issue_date", attributes.issueDate)
                    .field("expiry_date", attributes.expiryDate)
                    .field("feature", attributes.featureType)
                    .field("max_nodes", attributes.maxNodes)
                    .endObject();
        }
        licenses.endArray();
        licenses.endObject();
        return licenses.string();

    }

    public static String runLicenseGenerationTool(String licenseInput, String pubKeyPath, String priKeyPath) throws IOException, ParseException {
        String args[] = new String[6];
        args[0] = "--license";
        args[1] = licenseInput;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;

        return runLicenseGenerationTool(args);
    }

    public static String runLicenseGenerationTool(String[] args) throws IOException, ParseException {
        File temp = File.createTempFile("temp", ".out");
        temp.deleteOnExit();
        try (FileOutputStream outputStream = new FileOutputStream(temp)) {
            LicenseGeneratorTool.run(args, outputStream);
        }
        return FileUtils.readFileToString(temp);
    }

    public static void verifyESLicenses(Set<ESLicense> esLicenses, Map<String, FeatureAttributes> featureAttributesMap) throws ParseException {
        verifyESLicenses(ESLicenses.reduceAndMap(esLicenses), featureAttributesMap);

    }

    public static void verifyESLicenses(Map<String, ESLicense> esLicenses, Map<String, FeatureAttributes> featureAttributes) throws ParseException {
        assertTrue("Number of feature licenses should be " + featureAttributes.size(), esLicenses.size() == featureAttributes.size());
        for (Map.Entry<String, FeatureAttributes> featureAttrTuple : featureAttributes.entrySet()) {
            String featureType = featureAttrTuple.getKey();
            FeatureAttributes attributes = featureAttrTuple.getValue();
            final ESLicense esLicense = esLicenses.get(featureType);
            assertTrue("license for " + featureType + " should be present", esLicense != null);
            assertTrue("expected value for issuedTo was: " + attributes.issuedTo + " but got: " + esLicense.issuedTo(), esLicense.issuedTo().equals(attributes.issuedTo));
            assertTrue("expected value for type was: " + attributes.type + " but got: " + esLicense.type(), esLicense.type().equals(attributes.type));
            assertTrue("expected value for subscriptionType was: " + attributes.subscriptionType + " but got: " + esLicense.subscriptionType(), esLicense.subscriptionType().equals(attributes.subscriptionType));
            assertTrue("expected value for feature was: " + attributes.featureType + " but got: " + esLicense.feature(), esLicense.feature().equals(attributes.featureType));
            assertTrue("expected value for issueDate was: " + DateUtils.beginningOfTheDay(attributes.issueDate) + " but got: " + esLicense.issueDate(), esLicense.issueDate() == DateUtils.beginningOfTheDay(attributes.issueDate));
            assertTrue("expected value for expiryDate: " + DateUtils.endOfTheDay(attributes.expiryDate) + " but got: " + esLicense.expiryDate(), esLicense.expiryDate() == DateUtils.endOfTheDay(attributes.expiryDate));
            assertTrue("expected value for maxNodes: " + attributes.maxNodes + " but got: " + esLicense.maxNodes(), esLicense.maxNodes() == attributes.maxNodes);

            assertTrue("generated licenses should have non-null uid field", esLicense.uid() != null);
            assertTrue("generated licenses should have non-null signature field", esLicense.signature() != null);
        }
    }

    public static void isSame(Collection<ESLicense> firstLicenses, Collection<ESLicense> secondLicenses) {
        isSame(new HashSet<>(firstLicenses), new HashSet<>(secondLicenses));
    }

    public static void isSame(Set<ESLicense> firstLicenses, Set<ESLicense> secondLicenses) {

        // we do the verifyAndBuild to make sure we weed out any expired licenses
        final Map<String, ESLicense> licenses1 = ESLicenses.reduceAndMap(firstLicenses);
        final Map<String, ESLicense> licenses2 = ESLicenses.reduceAndMap(secondLicenses);

        // check if the effective licenses have the same feature set
        assertTrue("Both licenses should have the same number of features", licenses1.size() == licenses2.size());


        // for every feature license, check if all the attributes are the same
        for (String featureType : licenses1.keySet()) {
            ESLicense license1 = licenses1.get(featureType);
            ESLicense license2 = licenses2.get(featureType);

            isSame(license1, license2);

        }
    }

    public static void isSame(ESLicense license1, ESLicense license2) {

        assertTrue("Should have same uid; got: " + license1.uid() + " and " + license2.uid(), license1.uid().equals(license2.uid()));
        assertTrue("Should have same feature; got: " + license1.feature() + " and " + license2.feature(), license1.feature().equals(license2.feature()));
        assertTrue("Should have same subscriptType; got: " + license1.subscriptionType() + " and " + license2.subscriptionType(), license1.subscriptionType().equals(license2.subscriptionType()));
        assertTrue("Should have same type; got: " + license1.type() + " and " + license2.type(), license1.type().equals(license2.type()));
        assertTrue("Should have same issuedTo; got: " + license1.issuedTo() + " and " + license2.issuedTo(), license1.issuedTo().equals(license2.issuedTo()));
        assertTrue("Should have same signature; got: " + license1.signature() + " and " + license2.signature(), license1.signature().equals(license2.signature()));
        assertTrue("Should have same expiryDate; got: " + license1.expiryDate() + " and " + license2.expiryDate(), license1.expiryDate() == license2.expiryDate());
        assertTrue("Should have same issueDate; got: " + license1.issueDate() + " and " + license2.issueDate(), license1.issueDate() == license2.issueDate());
        assertTrue("Should have same maxNodes; got: " + license1.maxNodes() + " and " + license2.maxNodes(), license1.maxNodes() == license2.maxNodes());
    }

    public static class FeatureAttributes {

        public final String uid;
        public final String featureType;
        public final String type;
        public final String subscriptionType;
        public final String issuedTo;
        public final int maxNodes;
        public final String issueDate;
        public final String expiryDate;
        public final String issuer;

        public FeatureAttributes(String featureType, String type, String subscriptionType, String issuedTo, String issuer, int maxNodes, String issueDateStr, String expiryDateStr) throws ParseException {
            this(UUID.randomUUID().toString(), featureType, type, subscriptionType, issuedTo, issuer, maxNodes, issueDateStr, expiryDateStr);
        }

        public FeatureAttributes(String uid, String featureType, String type, String subscriptionType, String issuedTo, String issuer, int maxNodes, String issueDateStr, String expiryDateStr) throws ParseException {
            this.uid = uid;
            this.featureType = featureType;
            this.type = type;
            this.subscriptionType = subscriptionType;
            this.issuedTo = issuedTo;
            this.issuer = issuer;
            this.maxNodes = maxNodes;
            this.issueDate = issueDateStr;
            this.expiryDate = expiryDateStr;
        }
    }
}
