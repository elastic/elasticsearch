/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.protocol.xpack;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.BuildInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;

public class XPackInfoResponseTests extends AbstractStreamableXContentTestCase<XPackInfoResponse> {
    @Override
    protected XPackInfoResponse doParseInstance(XContentParser parser) {
        return XPackInfoResponse.fromXContent(parser);
    }

    @Override
    protected XPackInfoResponse createBlankInstance() {
        return new XPackInfoResponse();
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
        Function<XPackInfoResponse, XPackInfoResponse> mutator = randomFrom(
            r -> new XPackInfoResponse(
                    mutateBuildInfo(r.getBuildInfo()),
                    r.getLicenseInfo(),
                    r.getFeatureSetInfo()),
            r -> new XPackInfoResponse(
                    r.getBuildInfo(),
                    mutateLicenseInfo(r.getLicenseInfo()),
                    r.getFeatureSetInfo()),
            r -> new XPackInfoResponse(
                    r.getBuildInfo(),
                    r.getLicenseInfo(),
                    mutateFeatureSetInfo(r.getFeatureSetInfo())));
        return mutator.accept(response);
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
            randomAlphaOfLength(2),
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
        Map<String, FeatureSet> featureSets = new HashMap<>(size);
        while (featureSize.size() < size) {
            featureSets.put(randomAlphaOfLength(5), randomFeatureSet());
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
