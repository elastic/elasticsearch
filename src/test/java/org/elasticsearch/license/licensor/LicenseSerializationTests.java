/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.license.AbstractLicensingTestBase;
import org.elasticsearch.license.core.DateUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
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
        String licenseSpecs = generateESLicenseSpecString(Arrays.asList(new LicenseSpec("shield", issueDate, expiryDate)));
        Set<ESLicense> esLicensesOutput = new HashSet<>(ESLicenses.fromSource(licenseSpecs.getBytes(StandardCharsets.UTF_8), false));
        ESLicense generatedLicense = esLicensesOutput.iterator().next();

        assertThat(esLicensesOutput.size(), equalTo(1));
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
        String licenseSpecs = generateESLicenseSpecString(Arrays.asList(new LicenseSpec("shield", shieldIssueDate, shieldExpiryDate)));
        String licenseSpecs1 = generateESLicenseSpecString(Arrays.asList(new LicenseSpec("marvel", marvelIssueDate, marvelExpiryDate)));
        Set<ESLicense> esLicensesOutput = new HashSet<>();
        esLicensesOutput.addAll(ESLicenses.fromSource(licenseSpecs.getBytes(StandardCharsets.UTF_8), false));
        esLicensesOutput.addAll(ESLicenses.fromSource(licenseSpecs1.getBytes(StandardCharsets.UTF_8), false));
        assertThat(esLicensesOutput.size(), equalTo(2));
        for (ESLicense esLicense : esLicensesOutput) {
            assertThat(esLicense.issueDate(), equalTo(DateUtils.beginningOfTheDay((esLicense.feature().equals("shield")) ? shieldIssueDate : marvelIssueDate)));
            assertThat(esLicense.expiryDate(), equalTo(DateUtils.endOfTheDay((esLicense.feature().equals("shield")) ? shieldExpiryDate : marvelExpiryDate)));
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
        String licenseSpecsSource = generateESLicenseSpecString(specs);
        Set<ESLicense> esLicensesOutput = new HashSet<>(ESLicenses.fromSource(licenseSpecsSource.getBytes(StandardCharsets.UTF_8), false));
        assertThat(esLicensesOutput.size(), equalTo(licenseSpecs.size()));

        for (ESLicense license : esLicensesOutput) {
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
        Set<ESLicense> licenses = generateSignedLicenses(Arrays.asList(new LicenseSpec("expired_feature", validLicenseIssueDate, expiredLicenseExpiryDate)
                , new LicenseSpec("valid_feature", validLicenseIssueDate, validLicenseExpiryDate)));

        assertThat(licenses.size(), equalTo(2));
        for (ESLicense license : licenses) {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            license.toXContent(builder, new ToXContent.MapParams(ImmutableMap.of(ESLicenses.REST_VIEW_MODE, "true")));
            builder.flush();
            Map<String, Object> map = XContentHelper.convertToMap(builder.bytesStream().bytes(), false).v2();
            assertThat(map.get("status"), notNullValue());
            if (license.feature().equals("valid_feature")) {
                assertThat((String) map.get("status"), equalTo("active"));
            } else {
                assertThat((String) map.get("status"), equalTo("expired"));
            }
        }

        for (ESLicense license : licenses) {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            license.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
            Map<String, Object> map = XContentHelper.convertToMap(builder.bytesStream().bytes(), false).v2();
            assertThat(map.get("status"), nullValue());
        }
    }
}
