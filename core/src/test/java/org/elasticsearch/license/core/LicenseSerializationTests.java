/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

public class LicenseSerializationTests extends ElasticsearchTestCase {

    @Test
    public void testSimpleIssueExpiryDate() throws Exception {
        long now = System.currentTimeMillis();
        String issueDate = TestUtils.dateMathString("now", now);
        String expiryDate = TestUtils.dateMathString("now+10d/d", now);
        String licenseSpecs = TestUtils.generateLicenseSpecString(Arrays.asList(new TestUtils.LicenseSpec("shield", issueDate, expiryDate)));
        Set<License> licensesOutput = new HashSet<>(Licenses.fromSource(licenseSpecs.getBytes(StandardCharsets.UTF_8), false));
        License generatedLicense = licensesOutput.iterator().next();

        assertThat(licensesOutput.size(), equalTo(1));
        assertThat(generatedLicense.issueDate(), equalTo(DateUtils.beginningOfTheDay(issueDate)));
        assertThat(generatedLicense.expiryDate(), equalTo(DateUtils.endOfTheDay(expiryDate)));
    }

    @Test
    public void testMultipleIssueExpiryDate() throws Exception {
        long now = System.currentTimeMillis();
        String shieldIssueDate = TestUtils.dateMathString("now", now);
        String shieldExpiryDate = TestUtils.dateMathString("now+30d/d", now);
        String marvelIssueDate = TestUtils.dateMathString("now", now);
        String marvelExpiryDate = TestUtils.dateMathString("now+60d/d", now);
        String licenseSpecs = TestUtils.generateLicenseSpecString(Arrays.asList(new TestUtils.LicenseSpec("shield", shieldIssueDate, shieldExpiryDate)));
        String licenseSpecs1 = TestUtils.generateLicenseSpecString(Arrays.asList(new TestUtils.LicenseSpec("marvel", marvelIssueDate, marvelExpiryDate)));
        Set<License> licensesOutput = new HashSet<>();
        licensesOutput.addAll(Licenses.fromSource(licenseSpecs.getBytes(StandardCharsets.UTF_8), false));
        licensesOutput.addAll(Licenses.fromSource(licenseSpecs1.getBytes(StandardCharsets.UTF_8), false));
        assertThat(licensesOutput.size(), equalTo(2));
        for (License license : licensesOutput) {
            assertThat(license.issueDate(), equalTo(DateUtils.beginningOfTheDay((license.feature().equals("shield")) ? shieldIssueDate : marvelIssueDate)));
            assertThat(license.expiryDate(), equalTo(DateUtils.endOfTheDay((license.feature().equals("shield")) ? shieldExpiryDate : marvelExpiryDate)));
        }
    }

    @Test
    public void testLicensesFields() throws Exception {
        Map<String, TestUtils.LicenseSpec> licenseSpecs = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            TestUtils.LicenseSpec randomLicenseSpec = TestUtils.generateRandomLicenseSpec();
            licenseSpecs.put(randomLicenseSpec.feature, randomLicenseSpec);
        }

        ArrayList<TestUtils.LicenseSpec> specs = new ArrayList<>(licenseSpecs.values());
        String licenseSpecsSource = TestUtils.generateLicenseSpecString(specs);
        Set<License> licensesOutput = new HashSet<>(Licenses.fromSource(licenseSpecsSource.getBytes(StandardCharsets.UTF_8), false));
        assertThat(licensesOutput.size(), equalTo(licenseSpecs.size()));

        for (License license : licensesOutput) {
            TestUtils.LicenseSpec spec = licenseSpecs.get(license.feature());
            assertThat(spec, notNullValue());
            TestUtils.assertLicenseSpec(spec, license);
        }
    }

    @Test
    public void testLicenseRestView() throws Exception {
        long now = System.currentTimeMillis();
        String expiredLicenseExpiryDate = TestUtils.dateMathString("now-1d/d", now);
        String validLicenseIssueDate = TestUtils.dateMathString("now-10d/d", now);
        String validLicenseExpiryDate = TestUtils.dateMathString("now+1d/d", now);

        Set<License> licenses = TestUtils.generateLicenses(Arrays.asList(new TestUtils.LicenseSpec("expired_feature", validLicenseIssueDate, expiredLicenseExpiryDate)
                , new TestUtils.LicenseSpec("valid_feature", validLicenseIssueDate, validLicenseExpiryDate)));

        assertThat(licenses.size(), equalTo(2));
        for (License license : licenses) {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            license.toXContent(builder, new ToXContent.MapParams(ImmutableMap.of(Licenses.REST_VIEW_MODE, "true")));
            builder.flush();
            Map<String, Object> map = XContentHelper.convertToMap(builder.bytesStream().bytes(), false).v2();

            // should have an extra status field, human readable issue_data and expiry_date
            assertThat(map.get("status"), notNullValue());
            assertThat(map.get("issue_date"), notNullValue());
            assertThat(map.get("expiry_date"), notNullValue());
            if (license.feature().equals("valid_feature")) {
                assertThat((String) map.get("status"), equalTo("active"));
            } else {
                assertThat((String) map.get("status"), equalTo("expired"));
            }
        }

        for (License license : licenses) {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            license.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
            Map<String, Object> map = XContentHelper.convertToMap(builder.bytesStream().bytes(), false).v2();
            assertThat(map.get("status"), nullValue());
        }
    }
}
