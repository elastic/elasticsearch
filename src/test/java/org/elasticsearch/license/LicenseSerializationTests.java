/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.license.AbstractLicensingTestBase;
import org.elasticsearch.license.core.DateUtils;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class LicenseSerializationTests extends AbstractLicensingTestBase {

    @Test
    public void testSimpleIssueExpiryDate() throws Exception {
        long now = System.currentTimeMillis();
        String issueDate = dateMathString("now", now);
        String expiryDate = dateMathString("now+10d/d", now);
        String licenseSpecs = generateLicenseSpecString(Arrays.asList(new LicenseSpec("shield", issueDate, expiryDate)));
        Set<License> licensesOutput = new HashSet<>(Licenses.fromSource(licenseSpecs.getBytes(StandardCharsets.UTF_8), false));
        License generatedLicense = licensesOutput.iterator().next();

        assertThat(licensesOutput.size(), equalTo(1));
        assertThat(generatedLicense.issueDate(), equalTo(DateUtils.beginningOfTheDay(issueDate)));
        assertThat(generatedLicense.expiryDate(), equalTo(DateUtils.endOfTheDay(expiryDate)));
    }

    @Test
    public void testMultipleIssueExpiryDate() throws Exception {
        long now = System.currentTimeMillis();
        String shieldIssueDate = dateMathString("now", now);
        String shieldExpiryDate = dateMathString("now+30d/d", now);
        String marvelIssueDate = dateMathString("now", now);
        String marvelExpiryDate = dateMathString("now+60d/d", now);
        String licenseSpecs = generateLicenseSpecString(Arrays.asList(new LicenseSpec("shield", shieldIssueDate, shieldExpiryDate)));
        String licenseSpecs1 = generateLicenseSpecString(Arrays.asList(new LicenseSpec("marvel", marvelIssueDate, marvelExpiryDate)));
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
        Map<String, LicenseSpec> licenseSpecs = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            LicenseSpec randomLicenseSpec = generateRandomLicenseSpec();
            licenseSpecs.put(randomLicenseSpec.feature, randomLicenseSpec);
        }

        ArrayList<LicenseSpec> specs = new ArrayList<>(licenseSpecs.values());
        String licenseSpecsSource = generateLicenseSpecString(specs);
        Set<License> licensesOutput = new HashSet<>(Licenses.fromSource(licenseSpecsSource.getBytes(StandardCharsets.UTF_8), false));
        assertThat(licensesOutput.size(), equalTo(licenseSpecs.size()));

        for (License license : licensesOutput) {
            LicenseSpec spec = licenseSpecs.get(license.feature());
            assertThat(spec, notNullValue());
            assertLicenseSpec(spec, license);
        }
    }

    @Test
    public void testLicenseRestView() throws Exception {
        long now = System.currentTimeMillis();
        String expiredLicenseExpiryDate = dateMathString("now-1d/d", now);
        String validLicenseIssueDate = dateMathString("now-10d/d", now);
        String validLicenseExpiryDate = dateMathString("now+1d/d", now);
        Set<License> licenses = generateSignedLicenses(Arrays.asList(new LicenseSpec("expired_feature", validLicenseIssueDate, expiredLicenseExpiryDate)
                , new LicenseSpec("valid_feature", validLicenseIssueDate, validLicenseExpiryDate)));

        assertThat(licenses.size(), equalTo(2));
        for (License license : licenses) {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            license.toXContent(builder, new ToXContent.MapParams(ImmutableMap.of(Licenses.REST_VIEW_MODE, "true")));
            builder.flush();
            Map<String, Object> map = XContentHelper.convertToMap(builder.bytesStream().bytes(), false).v2();
            assertThat(map.get("status"), notNullValue());
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
