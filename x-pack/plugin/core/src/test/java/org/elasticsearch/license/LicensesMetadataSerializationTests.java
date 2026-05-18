/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.internal.TrialLicenseVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class LicensesMetadataSerializationTests extends ESTestCase {

    public void testXContentSerializationOneSignedLicense() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(2));
        LicensesMetadata licensesMetadata = new LicensesMetadata(license, null);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        ChunkedToXContent.wrapAsToXContent(licensesMetadata).toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        LicensesMetadata licensesMetadataFromXContent = getLicensesMetadataFromXContent(createParser(builder));
        assertThat(licensesMetadataFromXContent.getLicense(), equalTo(license));
        assertNull(licensesMetadataFromXContent.getMostRecentTrialVersion());
    }

    public void testXContentSerializationOneSignedLicenseWithUsedTrial() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(2));
        LicensesMetadata licensesMetadata = new LicensesMetadata(license, TrialLicenseVersion.CURRENT);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        ChunkedToXContent.wrapAsToXContent(licensesMetadata).toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        LicensesMetadata licensesMetadataFromXContent = getLicensesMetadataFromXContent(createParser(builder));
        assertThat(licensesMetadataFromXContent.getLicense(), equalTo(license));
        assertEquals(licensesMetadataFromXContent.getMostRecentTrialVersion(), TrialLicenseVersion.CURRENT);
    }

    public void testLicenseMetadataParsingDoesNotSwallowOtherMetadata() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(2));
        LicensesMetadata licensesMetadata = new LicensesMetadata(license, TrialLicenseVersion.CURRENT);
        NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(Map.of());

        final Metadata.Builder metadataBuilder = Metadata.builder();
        if (randomBoolean()) { // random order of insertion
            metadataBuilder.putCustom(licensesMetadata.getWriteableName(), licensesMetadata);
            metadataBuilder.putCustom(nodesShutdownMetadata.getWriteableName(), nodesShutdownMetadata);
        } else {
            metadataBuilder.putCustom(nodesShutdownMetadata.getWriteableName(), nodesShutdownMetadata);
            metadataBuilder.putCustom(licensesMetadata.getWriteableName(), licensesMetadata);
        }
        // serialize metadata
        XContentBuilder builder = XContentFactory.jsonBuilder();
        Params params = new ToXContent.MapParams(Collections.singletonMap(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));
        builder.startObject();
        builder = ChunkedToXContent.wrapAsToXContent(metadataBuilder.build()).toXContent(builder, params);
        builder.endObject();
        // deserialize metadata again
        Metadata metadata = Metadata.Builder.fromXContent(createParser(builder));
        // check that custom metadata still present
        assertThat(metadata.custom(licensesMetadata.getWriteableName()), notNullValue());
        assertThat(metadata.custom(nodesShutdownMetadata.getWriteableName()), notNullValue());
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
        LicensesMetadata licensesMetadata = new LicensesMetadata(trialLicense, TrialLicenseVersion.CURRENT);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        ChunkedToXContent.wrapAsToXContent(licensesMetadata).toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        LicensesMetadata licensesMetadataFromXContent = getLicensesMetadataFromXContent(createParser(builder));
        assertThat(licensesMetadataFromXContent.getLicense(), equalTo(trialLicense));
        assertEquals(licensesMetadataFromXContent.getMostRecentTrialVersion(), TrialLicenseVersion.CURRENT);
    }

    public void testLicenseTombstoneFromXContext() throws Exception {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        builder.nullField("license");
        builder.endObject();
        builder.endObject();
        LicensesMetadata metadataFromXContent = getLicensesMetadataFromXContent(createParser(builder));
        assertThat(metadataFromXContent.getLicense(), equalTo(LicensesMetadata.LICENSE_TOMBSTONE));
    }

    public void testLicenseTombstoneWithUsedTrialFromXContext() throws Exception {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("licenses");
        builder.nullField("license");
        builder.field("trial_license", TrialLicenseVersion.CURRENT);
        builder.endObject();
        builder.endObject();
        LicensesMetadata metadataFromXContent = getLicensesMetadataFromXContent(createParser(builder));
        assertThat(metadataFromXContent.getLicense(), equalTo(LicensesMetadata.LICENSE_TOMBSTONE));
        assertEquals(metadataFromXContent.getMostRecentTrialVersion(), TrialLicenseVersion.CURRENT);
    }

    private static LicensesMetadata getLicensesMetadataFromXContent(XContentParser parser) throws Exception {
        parser.nextToken(); // consume null
        parser.nextToken(); // consume "licenses"
        LicensesMetadata licensesMetadataFromXContent = LicensesMetadata.fromXContent(parser);
        parser.nextToken(); // consume endObject
        assertThat(parser.nextToken(), nullValue());
        return licensesMetadataFromXContent;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            CollectionUtils.concatLists(new XPackPlugin(Settings.EMPTY).getNamedXContent(), ClusterModule.getNamedXWriteables())
        );
    }
}
