/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

public class LicenseSerializationTests extends ESTestCase {
    public void testSimpleIssueExpiryDate() throws Exception {
        long now = System.currentTimeMillis();
        String issueDate = TestUtils.dateMathString("now", now);
        String expiryDate = TestUtils.dateMathString("now+10d/d", now);
        String licenseSpecs = TestUtils.generateLicenseSpecString(new TestUtils.LicenseSpec(issueDate, expiryDate));
        License generatedLicense = License.fromSource(new BytesArray(licenseSpecs.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        assertThat(generatedLicense.issueDate(), equalTo(DateUtils.beginningOfTheDay(issueDate)));
        assertThat(generatedLicense.expiryDate(), equalTo(DateUtils.endOfTheDay(expiryDate)));
    }

    public void testLicensesFields() throws Exception {
        TestUtils.LicenseSpec randomLicenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_START);
        String licenseSpecsSource = TestUtils.generateLicenseSpecString(randomLicenseSpec);
        final License fromSource =
                License.fromSource(new BytesArray(licenseSpecsSource.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        TestUtils.assertLicenseSpec(randomLicenseSpec, fromSource);
    }

    public void testLicenseRestView() throws Exception {
        long now = System.currentTimeMillis();
        String expiredLicenseExpiryDate = TestUtils.dateMathString("now-1d/d", now);
        String validLicenseIssueDate = TestUtils.dateMathString("now-10d/d", now);
        String invalidLicenseIssueDate = TestUtils.dateMathString("now+1d/d", now);
        String validLicenseExpiryDate = TestUtils.dateMathString("now+2d/d", now);

        License license = TestUtils.generateLicenses(new TestUtils.LicenseSpec(validLicenseIssueDate, expiredLicenseExpiryDate));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        license.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap(License.REST_VIEW_MODE, "true")));
        builder.flush();
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        // should have an extra status field, human readable issue_data and expiry_date
        assertThat(map.get("status"), notNullValue());
        assertThat(map.get("issue_date"), notNullValue());
        assertThat(map.get("expiry_date"), notNullValue());
        assertThat(map.get("status"), equalTo("expired"));
        builder = XContentFactory.contentBuilder(XContentType.JSON);
        license.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(map.get("status"), nullValue());

        license = TestUtils.generateLicenses(new TestUtils.LicenseSpec(validLicenseIssueDate, validLicenseExpiryDate));
        builder = XContentFactory.contentBuilder(XContentType.JSON);
        license.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap(License.REST_VIEW_MODE, "true")));
        builder.flush();
        map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        // should have an extra status field, human readable issue_data and expiry_date
        assertThat(map.get("status"), notNullValue());
        assertThat(map.get("issue_date"), notNullValue());
        assertThat(map.get("expiry_date"), notNullValue());
        assertThat(map.get("status"), equalTo("active"));
        builder = XContentFactory.contentBuilder(XContentType.JSON);
        license.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(map.get("status"), nullValue());

        license = TestUtils.generateLicenses(new TestUtils.LicenseSpec(invalidLicenseIssueDate, validLicenseExpiryDate));
        builder = XContentFactory.contentBuilder(XContentType.JSON);
        license.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap(License.REST_VIEW_MODE, "true")));
        builder.flush();
        map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        // should have an extra status field, human readable issue_data and expiry_date
        assertThat(map.get("status"), notNullValue());
        assertThat(map.get("issue_date"), notNullValue());
        assertThat(map.get("expiry_date"), notNullValue());
        assertThat(map.get("status"), equalTo("invalid"));
        builder = XContentFactory.contentBuilder(XContentType.JSON);
        license.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(map.get("status"), nullValue());
    }

    public void testLicenseRestViewNonExpiringBasic() throws Exception {
        long now = System.currentTimeMillis();

        License.Builder specBuilder = License.builder()
                .uid(UUID.randomUUID().toString())
                .issuedTo("test")
                .maxNodes(1000)
                .issueDate(now)
                .type("basic")
                .expiryDate(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS);
        License license = SelfGeneratedLicense.create(specBuilder, License.VERSION_CURRENT);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        license.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap(License.REST_VIEW_MODE, "true")));
        builder.flush();
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        // should have an extra status field, human readable issue_data and no expiry_date
        assertThat(map.get("status"), notNullValue());
        assertThat(map.get("type"), equalTo("basic"));
        assertThat(map.get("issue_date"), notNullValue());
        assertThat(map.get("expiry_date"), nullValue());
        assertThat(map.get("expiry_date_in_millis"), nullValue());
        assertThat(map.get("status"), equalTo("active"));
        builder = XContentFactory.contentBuilder(XContentType.JSON);
        license.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(map.get("status"), nullValue());
    }
}
