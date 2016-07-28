/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.License;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class LicensesMetaDataSerializationTests extends ESTestCase {
    public void testXContentSerializationOneSignedLicense() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(2));
        LicensesMetaData licensesMetaData = new LicensesMetaData(license);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject("licenses");
        licensesMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(builder.bytes());
        assertThat(licensesMetaDataFromXContent.getLicense(), equalTo(license));
    }

    public void testLicenseMetadataParsingDoesNotSwallowOtherMetaData() throws Exception {
        new Licensing(Settings.EMPTY); // makes sure LicensePlugin is registered in Custom MetaData
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(2));
        LicensesMetaData licensesMetaData = new LicensesMetaData(license);
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("repo", "fs", Settings.EMPTY);
        RepositoriesMetaData repositoriesMetaData = new RepositoriesMetaData(repositoryMetaData);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        if (randomBoolean()) { // random order of insertion
            metaDataBuilder.putCustom(licensesMetaData.type(), licensesMetaData);
            metaDataBuilder.putCustom(repositoriesMetaData.type(), repositoriesMetaData);
        } else {
            metaDataBuilder.putCustom(repositoriesMetaData.type(), repositoriesMetaData);
            metaDataBuilder.putCustom(licensesMetaData.type(), licensesMetaData);
        }
        // serialize metadata
        XContentBuilder builder = XContentFactory.jsonBuilder();
        Params params = new ToXContent.MapParams(Collections.singletonMap(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_GATEWAY));
        builder.startObject();
        builder = metaDataBuilder.build().toXContent(builder, params);
        builder.endObject();
        String serializedMetaData = builder.string();
        // deserialize metadata again
        MetaData metaData = MetaData.Builder.fromXContent(XContentFactory.xContent(XContentType.JSON).createParser(serializedMetaData));
        // check that custom metadata still present
        assertThat(metaData.custom(licensesMetaData.type()), notNullValue());
        assertThat(metaData.custom(repositoriesMetaData.type()), notNullValue());
    }

    public void testXContentSerializationOneTrial() throws Exception {
        long issueDate = System.currentTimeMillis();
        License.Builder specBuilder = License.builder()
                .uid(UUID.randomUUID().toString())
                .issuedTo("customer")
                .maxNodes(5)
                .issueDate(issueDate)
                .expiryDate(issueDate + TimeValue.timeValueHours(2).getMillis());
        final License trialLicense = TrialLicense.create(specBuilder);
        LicensesMetaData licensesMetaData = new LicensesMetaData(trialLicense);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject("licenses");
        licensesMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(builder.bytes());
        assertThat(licensesMetaDataFromXContent.getLicense(), equalTo(trialLicense));
    }

    public void testLicenseTombstoneFromXContext() throws Exception {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject("licenses");
        builder.nullField("license");
        builder.endObject();
        LicensesMetaData metaDataFromXContent = getLicensesMetaDataFromXContent(builder.bytes());
        assertThat(metaDataFromXContent.getLicense(), equalTo(LicensesMetaData.LICENSE_TOMBSTONE));
    }

    private static LicensesMetaData getLicensesMetaDataFromXContent(BytesReference bytes) throws Exception {
        final XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(bytes);
        parser.nextToken(); // consume null
        parser.nextToken(); // consume "licenses"
        LicensesMetaData licensesMetaDataFromXContent = LicensesMetaData.PROTO.fromXContent(parser);
        parser.nextToken(); // consume endObject
        assertThat(parser.nextToken(), nullValue());
        return licensesMetaDataFromXContent;
    }
}
