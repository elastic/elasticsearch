/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.BuildInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class XPackInfoResponseTests extends AbstractResponseTestCase<XPackInfoResponse, org.elasticsearch.client.xpack.XPackInfoResponse> {

    private BuildInfo convertHlrcToInternal(org.elasticsearch.client.xpack.XPackInfoResponse.BuildInfo buildInfo) {
        return buildInfo != null ? new BuildInfo(buildInfo.getHash(), buildInfo.getTimestamp()) : null;
    }

    private LicenseInfo convertHlrcToInternal(org.elasticsearch.client.xpack.XPackInfoResponse.LicenseInfo licenseInfo) {
        return licenseInfo != null
            ? new LicenseInfo(licenseInfo.getUid(), licenseInfo.getType(), licenseInfo.getMode(),
                licenseInfo.getStatus() != null ? LicenseStatus.valueOf(licenseInfo.getStatus().name()) : null,
                licenseInfo.getExpiryDate())
            : null;
    }

    private FeatureSetsInfo convertHlrcToInternal(org.elasticsearch.client.xpack.XPackInfoResponse.FeatureSetsInfo featureSetsInfo) {
        return featureSetsInfo != null
            ? new FeatureSetsInfo(featureSetsInfo.getFeatureSets().values().stream()
            .map(fs -> new FeatureSet(fs.name(), fs.available(), fs.enabled()))
            .collect(Collectors.toSet()))
            : null;
    }

    private BuildInfo randomBuildInfo() {
        return new BuildInfo(
            randomAlphaOfLength(10),
            randomAlphaOfLength(15));
    }

    private LicenseInfo randomLicenseInfo() {
        return new LicenseInfo(
            randomAlphaOfLength(10),
            randomAlphaOfLength(4),
            randomAlphaOfLength(5),
            randomFrom(LicenseStatus.values()),
            randomLong());
    }

    private FeatureSetsInfo randomFeatureSetsInfo() {
        int size = between(0, 10);
        Set<FeatureSet> featureSets = new HashSet<>(size);
        while (featureSets.size() < size) {
            featureSets.add(randomFeatureSet());
        }
        return new FeatureSetsInfo(featureSets);
    }

    private FeatureSet randomFeatureSet() {
        return new FeatureSet(
            randomAlphaOfLength(5),
            randomBoolean(),
            randomBoolean());
    }

    @Override
    protected XPackInfoResponse createServerTestInstance(XContentType xContentType) {
        return new XPackInfoResponse(
            randomBoolean() ? null : randomBuildInfo(),
            randomBoolean() ? null : randomLicenseInfo(),
            randomBoolean() ? null : randomFeatureSetsInfo());
    }

    @Override
    protected org.elasticsearch.client.xpack.XPackInfoResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.xpack.XPackInfoResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(XPackInfoResponse serverTestInstance, org.elasticsearch.client.xpack.XPackInfoResponse clientInstance) {
        XPackInfoResponse serverInstance = new XPackInfoResponse(convertHlrcToInternal(clientInstance.getBuildInfo()),
            convertHlrcToInternal(clientInstance.getLicenseInfo()), convertHlrcToInternal(clientInstance.getFeatureSetsInfo()));
        assertEquals(serverTestInstance, serverInstance);
    }
}
