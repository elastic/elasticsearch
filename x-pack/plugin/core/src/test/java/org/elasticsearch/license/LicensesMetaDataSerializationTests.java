/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class LicensesMetaDataSerializationTests extends ESTestCase {

    public void testXContentSerializationOneSignedLicense() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(2));
        LicensesMetaData licensesMetaData = new LicensesMetaData(license, null);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        licensesMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(createParser(builder));
        assertThat(licensesMetaDataFromXContent.getLicense(), equalTo(license));
        assertNull(licensesMetaDataFromXContent.getMostRecentTrialVersion());
    }

    public void testXContentSerializationOneSignedLicenseWithUsedTrial() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(2));
        LicensesMetaData licensesMetaData = new LicensesMetaData(license, Version.CURRENT);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        licensesMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(createParser(builder));
        assertThat(licensesMetaDataFromXContent.getLicense(), equalTo(license));
        assertEquals(licensesMetaDataFromXContent.getMostRecentTrialVersion(), Version.CURRENT);
    }

    public void testLicenseMetadataParsingDoesNotSwallowOtherMetaData() throws Exception {
        new Licensing(Settings.EMPTY); // makes sure LicensePlugin is registered in Custom MetaData
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(2));
        LicensesMetaData licensesMetaData = new LicensesMetaData(license, Version.CURRENT);
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("repo", "fs", Settings.EMPTY);
        RepositoriesMetaData repositoriesMetaData = new RepositoriesMetaData(Collections.singletonList(repositoryMetaData));
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        if (randomBoolean()) { // random order of insertion
            metaDataBuilder.putCustom(licensesMetaData.getWriteableName(), licensesMetaData);
            metaDataBuilder.putCustom(repositoriesMetaData.getWriteableName(), repositoriesMetaData);
        } else {
            metaDataBuilder.putCustom(repositoriesMetaData.getWriteableName(), repositoriesMetaData);
            metaDataBuilder.putCustom(licensesMetaData.getWriteableName(), licensesMetaData);
        }
        // serialize metadata
        XContentBuilder builder = XContentFactory.jsonBuilder();
        Params params = new ToXContent.MapParams(Collections.singletonMap(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_GATEWAY));
        builder.startObject();
        builder = metaDataBuilder.build().toXContent(builder, params);
        builder.endObject();
        // deserialize metadata again
        MetaData metaData = MetaData.Builder.fromXContent(createParser(builder), randomBoolean());
        // check that custom metadata still present
        assertThat(metaData.custom(licensesMetaData.getWriteableName()), notNullValue());
        assertThat(metaData.custom(repositoriesMetaData.getWriteableName()), notNullValue());
    }

    public void testXContentSerializationOneTrial() throws Exception {
        long issueDate = System.currentTimeMillis();
        License.Builder specBuilder = License.builder()
                .uid(UUID.randomUUID().toString())
                .issuedTo("customer")
                .maxNodes(5)
                .issueDate(issueDate)
                .type(randomBoolean() ? "trial" : "basic")
                .expiryDate(issueDate + TimeValue.timeValueHours(2).getMillis());
        final License trialLicense = SelfGeneratedLicense.create(specBuilder, License.VERSION_CURRENT);
        LicensesMetaData licensesMetaData = new LicensesMetaData(trialLicense, Version.CURRENT);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        licensesMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        LicensesMetaData licensesMetaDataFromXContent = getLicensesMetaDataFromXContent(createParser(builder));
        assertThat(licensesMetaDataFromXContent.getLicense(), equalTo(trialLicense));
        assertEquals(licensesMetaDataFromXContent.getMostRecentTrialVersion(), Version.CURRENT);
    }

    public void testLicenseTombstoneFromXContext() throws Exception {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        builder.nullField("license");
        builder.endObject();
        builder.endObject();
        LicensesMetaData metaDataFromXContent = getLicensesMetaDataFromXContent(createParser(builder));
        assertThat(metaDataFromXContent.getLicense(), equalTo(LicensesMetaData.LICENSE_TOMBSTONE));
    }

    public void testLicenseTombstoneWithUsedTrialFromXContext() throws Exception {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        builder.nullField("license");
        builder.field("trial_license", Version.CURRENT.toString());
        builder.endObject();
        builder.endObject();
        LicensesMetaData metaDataFromXContent = getLicensesMetaDataFromXContent(createParser(builder));
        assertThat(metaDataFromXContent.getLicense(), equalTo(LicensesMetaData.LICENSE_TOMBSTONE));
        assertEquals(metaDataFromXContent.getMostRecentTrialVersion(), Version.CURRENT);
    }

    private static LicensesMetaData getLicensesMetaDataFromXContent(XContentParser parser) throws Exception {
        parser.nextToken(); // consume null
        parser.nextToken(); // consume "licenses"
        LicensesMetaData licensesMetaDataFromXContent = LicensesMetaData.fromXContent(parser);
        parser.nextToken(); // consume endObject
        assertThat(parser.nextToken(), nullValue());
        return licensesMetaDataFromXContent;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(Stream.concat(
                new Licensing(Settings.EMPTY).getNamedXContent().stream(),
                ClusterModule.getNamedXWriteables().stream()
        ).collect(Collectors.toList()));
    }
}
