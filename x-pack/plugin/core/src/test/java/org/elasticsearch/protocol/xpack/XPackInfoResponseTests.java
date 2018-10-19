/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHLRCStreamableXContentTestCase;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.BuildInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.io.IOException;
import java.util.stream.Collectors;

public class XPackInfoResponseTests extends AbstractHLRCStreamableXContentTestCase<XPackInfoResponse,
    org.elasticsearch.client.xpack.XPackInfoResponse> {
    @Override
    protected XPackInfoResponse doParseInstance(XContentParser parser) throws IOException {
        return XPackInfoResponse.fromXContent(parser);
    }

    @Override
    protected XPackInfoResponse createBlankInstance() {
        return new XPackInfoResponse();
    }

    @Override
    public org.elasticsearch.client.xpack.XPackInfoResponse doHLRCParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.xpack.XPackInfoResponse.fromXContent(parser);
    }

    @Override
    public XPackInfoResponse convert(org.elasticsearch.client.xpack.XPackInfoResponse instance) {
        final org.elasticsearch.client.xpack.XPackInfoResponse.BuildInfo buildInfo = instance.getBuildInfo();
        final XPackInfoResponse.BuildInfo buildInfo1 =
            buildInfo != null ? new BuildInfo(buildInfo.getHash(), buildInfo.getTimestamp()) : null;
        final org.elasticsearch.client.xpack.XPackInfoResponse.LicenseInfo licenseInfo = instance.getLicenseInfo();
        final XPackInfoResponse.LicenseInfo licenseInfo1 =
            licenseInfo != null
            ? new LicenseInfo(licenseInfo.getUid(), licenseInfo.getType(), licenseInfo.getMode(),
                licenseInfo.getStatus() != null ? LicenseStatus.valueOf(licenseInfo.getStatus().name()) : null,
                licenseInfo.getExpiryDate())
            : null;
        final org.elasticsearch.client.xpack.XPackInfoResponse.FeatureSetsInfo featureSetsInfo = instance.getFeatureSetsInfo();
        final XPackInfoResponse.FeatureSetsInfo featureSetsInfo1 =
            featureSetsInfo != null
            ? new FeatureSetsInfo(featureSetsInfo.getFeatureSets().values().stream()
               .map(fs -> new XPackInfoResponse.FeatureSetsInfo.FeatureSet(fs.name(), fs.description(), fs.available(), fs.enabled(),
                   fs.nativeCodeInfo()))
               .collect(Collectors.toSet()))
            : null;
        return new XPackInfoResponse(buildInfo1, licenseInfo1, featureSetsInfo1);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return path -> path.equals("features")
                || (path.startsWith("features") && path.endsWith("native_code_info"));
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("human", randomBoolean() ? "true" : "false");
        }
        if (randomBoolean()) {
            params.put("categories", "_none");
        }
        return new ToXContent.MapParams(params);
    }

    @Override
    protected XPackInfoResponse createTestInstance() {
        return new XPackInfoResponse(
            randomBoolean() ? null : randomBuildInfo(),
            randomBoolean() ? null : randomLicenseInfo(),
            randomBoolean() ? null : randomFeatureSetsInfo());
    }

    @Override
    protected XPackInfoResponse mutateInstance(XPackInfoResponse response) {
        @SuppressWarnings("unchecked")
        Function<XPackInfoResponse, XPackInfoResponse> mutator = randomFrom(
            r -> new XPackInfoResponse(
                    mutateBuildInfo(r.getBuildInfo()),
                    r.getLicenseInfo(),
                    r.getFeatureSetsInfo()),
            r -> new XPackInfoResponse(
                    r.getBuildInfo(),
                    mutateLicenseInfo(r.getLicenseInfo()),
                    r.getFeatureSetsInfo()),
            r -> new XPackInfoResponse(
                    r.getBuildInfo(),
                    r.getLicenseInfo(),
                    mutateFeatureSetsInfo(r.getFeatureSetsInfo())));
        return mutator.apply(response);
    }

    private BuildInfo randomBuildInfo() {
        return new BuildInfo(
            randomAlphaOfLength(10),
            randomAlphaOfLength(15));
    }

    private BuildInfo mutateBuildInfo(BuildInfo buildInfo) {
        if (buildInfo == null) {
            return randomBuildInfo();
        }
        return null;
    }

    private LicenseInfo randomLicenseInfo() {
        return new LicenseInfo(
            randomAlphaOfLength(10),
            randomAlphaOfLength(4),
            randomAlphaOfLength(5),
            randomFrom(LicenseStatus.values()),
            randomLong());
    }

    private LicenseInfo mutateLicenseInfo(LicenseInfo licenseInfo) {
        if (licenseInfo == null) {
            return randomLicenseInfo();
        }
        return null;
    }

    private FeatureSetsInfo randomFeatureSetsInfo() {
        int size = between(0, 10);
        Set<FeatureSet> featureSets = new HashSet<>(size);
        while (featureSets.size() < size) {
            featureSets.add(randomFeatureSet());
        }
        return new FeatureSetsInfo(featureSets);
    }

    private FeatureSetsInfo mutateFeatureSetsInfo(FeatureSetsInfo featureSetsInfo) {
        if (featureSetsInfo == null) {
            return randomFeatureSetsInfo();
        }
        return null;
    }

    private FeatureSet randomFeatureSet() {
        return new FeatureSet(
            randomAlphaOfLength(5),
            randomBoolean() ? null : randomAlphaOfLength(20),
            randomBoolean(),
            randomBoolean(),
            randomNativeCodeInfo());
    }

    private Map<String, Object> randomNativeCodeInfo() {
        if (randomBoolean()) {
            return null;
        }
        int size = between(0, 10);
        Map<String, Object> nativeCodeInfo = new HashMap<>(size);
        while (nativeCodeInfo.size() < size) {
            nativeCodeInfo.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return nativeCodeInfo;
    }
}
