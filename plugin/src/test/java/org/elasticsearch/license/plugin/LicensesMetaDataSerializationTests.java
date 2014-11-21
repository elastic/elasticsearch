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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.license.plugin.core.TrialLicenseUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LicensesMetaDataSerializationTests extends ElasticsearchTestCase {

    @Test
    public void testXContentSerializationOneSignedLicense() throws Exception {
        License license = TestUtils.generateSignedLicense("feature", TimeValue.timeValueHours(2));
        LicensesMetaData licensesMetaData = new LicensesMetaData(Arrays.asList(license), new ArrayList<License>());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject("licensesMetaData");
        LicensesMetaData.FACTORY.toXContent(licensesMetaData, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] serializedBytes = builder.bytes().toBytes();

        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(serializedBytes);

        assertThat(licensesMetaDataFromXContent.getSignedLicenses().size(), equalTo(1));
        TestUtils.isSame(licensesMetaDataFromXContent.getSignedLicenses().get(0), license);
        assertThat(licensesMetaDataFromXContent.getTrialLicenses().size(), equalTo(0));
    }

    @Test
    public void testXContentSerializationManySignedLicense() throws Exception {
        List<License> licenses = new ArrayList<>();
        int n = randomIntBetween(2, 5);
        for (int i = 0; i < n; i++) {
            licenses.add(TestUtils.generateSignedLicense("feature__" + String.valueOf(i), TimeValue.timeValueHours(2)));
        }
        LicensesMetaData licensesMetaData = new LicensesMetaData(licenses, new ArrayList<License>());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject("licensesMetaData");
        LicensesMetaData.FACTORY.toXContent(licensesMetaData, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] serializedBytes = builder.bytes().toBytes();

        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(serializedBytes);

        assertThat(licensesMetaDataFromXContent.getSignedLicenses().size(), equalTo(n));
        TestUtils.isSame(licensesMetaDataFromXContent.getSignedLicenses(), licenses);
        assertThat(licensesMetaDataFromXContent.getTrialLicenses().size(), equalTo(0));
    }

    @Test
    public void testXContentSerializationOneTrial() throws Exception {
        final License trialLicense = TrialLicenseUtils.builder()
                .feature("feature")
                .duration(TimeValue.timeValueHours(2))
                .maxNodes(5)
                .issuedTo("customer")
                .issueDate(System.currentTimeMillis())
                .build();
        LicensesMetaData licensesMetaData = new LicensesMetaData(new ArrayList<License>(), Arrays.asList(trialLicense));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject("licensesMetaData");
        LicensesMetaData.FACTORY.toXContent(licensesMetaData, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] serializedBytes = builder.bytes().toBytes();

        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(serializedBytes);

        assertThat(licensesMetaDataFromXContent.getTrialLicenses().size(), equalTo(1));
        TestUtils.isSame(licensesMetaDataFromXContent.getTrialLicenses().iterator().next(), trialLicense);
        assertThat(licensesMetaDataFromXContent.getSignedLicenses().size(), equalTo(0));
    }

    @Test
    public void testXContentSerializationManyTrial() throws Exception {
        final TrialLicenseUtils.TrialLicenseBuilder trialLicenseBuilder = TrialLicenseUtils.builder()
                .duration(TimeValue.timeValueHours(2))
                .maxNodes(5)
                .issuedTo("customer")
                .issueDate(System.currentTimeMillis());
        int n = randomIntBetween(2, 5);
        List<License> trialLicenses = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            trialLicenses.add(trialLicenseBuilder.feature("feature__" + String.valueOf(i)).build());
        }
        LicensesMetaData licensesMetaData = new LicensesMetaData(new ArrayList<License>(), trialLicenses);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject("licensesMetaData");
        LicensesMetaData.FACTORY.toXContent(licensesMetaData, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] serializedBytes = builder.bytes().toBytes();

        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(serializedBytes);

        assertThat(licensesMetaDataFromXContent.getTrialLicenses().size(), equalTo(n));
        TestUtils.isSame(licensesMetaDataFromXContent.getTrialLicenses(), trialLicenses);
        assertThat(licensesMetaDataFromXContent.getSignedLicenses().size(), equalTo(0));
    }

    @Test
    public void testXContentSerializationManyTrialAndSignedLicenses() throws Exception {
        final TrialLicenseUtils.TrialLicenseBuilder trialLicenseBuilder = TrialLicenseUtils.builder()
                .duration(TimeValue.timeValueHours(2))
                .maxNodes(5)
                .issuedTo("customer")
                .issueDate(System.currentTimeMillis());
        int n = randomIntBetween(2, 5);
        List<License> trialLicenses = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            trialLicenses.add(trialLicenseBuilder.feature("feature__" + String.valueOf(i)).build());
        }
        List<License> licenses = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            licenses.add(TestUtils.generateSignedLicense("feature__" + String.valueOf(i), TimeValue.timeValueHours(2)));
        }
        LicensesMetaData licensesMetaData = new LicensesMetaData(licenses, trialLicenses);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject("licensesMetaData");
        LicensesMetaData.FACTORY.toXContent(licensesMetaData, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] serializedBytes = builder.bytes().toBytes();

        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(serializedBytes);

        assertThat(licensesMetaDataFromXContent.getTrialLicenses().size(), equalTo(n));
        assertThat(licensesMetaDataFromXContent.getSignedLicenses().size(), equalTo(n));
        TestUtils.isSame(licensesMetaDataFromXContent.getTrialLicenses(), trialLicenses);
        TestUtils.isSame(licensesMetaDataFromXContent.getSignedLicenses(), licenses);
    }

    private static LicensesMetaData getLicensesMetaDataFromXContent(byte[] bytes) throws Exception {
        final XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        parser.nextToken(); // consume null
        parser.nextToken(); // consume "licensesMetaData"
        LicensesMetaData licensesMetaDataFromXContent = LicensesMetaData.FACTORY.fromXContent(parser);
        parser.nextToken(); // consume endObject
        assertThat(parser.nextToken(), nullValue());
        return licensesMetaDataFromXContent;
    }
}
