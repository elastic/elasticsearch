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

package org.elasticsearch.client.xpack;

import org.elasticsearch.client.license.LicenseStatus;
import org.elasticsearch.client.xpack.XPackInfoResponse.BuildInfo;
import org.elasticsearch.client.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.client.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.client.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class XPackInfoResponseTests extends AbstractXContentTestCase<XPackInfoResponse> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    protected XPackInfoResponse doParseInstance(XContentParser parser) throws IOException {
        return XPackInfoResponse.fromXContent(parser);
    }

    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return path -> path.equals("features")
                || (path.startsWith("features") && path.endsWith("native_code_info"));
    }

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

    protected XPackInfoResponse createTestInstance() {
        return new XPackInfoResponse(
            randomBoolean() ? null : randomBuildInfo(),
            randomBoolean() ? null : randomLicenseInfo(),
            randomBoolean() ? null : randomFeatureSetsInfo());
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
