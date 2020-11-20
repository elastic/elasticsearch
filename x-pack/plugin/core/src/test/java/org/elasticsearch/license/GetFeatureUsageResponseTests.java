/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;

public class GetFeatureUsageResponseTests extends ESTestCase {

    public void testSerializationCurrentVersion() throws Exception {
        final GetFeatureUsageResponse response = randomResponse();
        final Version version = Version.CURRENT;
        final GetFeatureUsageResponse read = copyResponse(response, version);
        assertThat(read.getFeatures(), hasSize(response.getFeatures().size()));
        for (int i = 0; i < response.getFeatures().size(); i++) {
            final GetFeatureUsageResponse.FeatureUsageInfo origFeature = response.getFeatures().get(i);
            final GetFeatureUsageResponse.FeatureUsageInfo readFeature = read.getFeatures().get(i);
            assertThat(readFeature.name, equalTo(origFeature.name));
            assertThat(readFeature.licenseLevel, equalTo(origFeature.licenseLevel));
            assertThat(readFeature.lastUsedTime, equalTo(origFeature.lastUsedTime));
            assertThat(readFeature.identifiers, equalTo(origFeature.identifiers));
        }
    }

    public void testSerializationVersion7_10() throws Exception {
        final GetFeatureUsageResponse response = randomResponse();
        final Version version = Version.V_7_10_0;
        final GetFeatureUsageResponse read = copyResponse(response, version);
        assertThat(read.getFeatures(), hasSize(response.getFeatures().size()));
        for (int i = 0; i < response.getFeatures().size(); i++) {
            final GetFeatureUsageResponse.FeatureUsageInfo origFeature = response.getFeatures().get(i);
            final GetFeatureUsageResponse.FeatureUsageInfo readFeature = read.getFeatures().get(i);
            assertThat(readFeature.name, equalTo(origFeature.name));
            assertThat(readFeature.licenseLevel, equalTo(origFeature.licenseLevel));
            assertThat(readFeature.lastUsedTime, equalTo(origFeature.lastUsedTime));
            assertThat(readFeature.identifiers, emptyIterable());
        }
    }

    public void testToXContent() throws Exception {
        final GetFeatureUsageResponse response = randomResponse();
        var map = XContentTestUtils.convertToMap(response);

        assertThat(map.get("features"), instanceOf(List.class));
        final List<?> features = (List<?>) map.get("features");
        assertThat(features, iterableWithSize(response.getFeatures().size()));

        for (int i = 0; i < features.size(); i++) {
            assertThat(features.get(i), instanceOf(Map.class));
            var read = (Map<String, ?>) features.get(i);
            final GetFeatureUsageResponse.FeatureUsageInfo orig = response.getFeatures().get(i);
            assertThat(read.get("name"), equalTo(orig.name));
            assertThat(read.get("license_level"), equalTo(orig.licenseLevel));
            assertThat(read.get("last_used"), equalTo(orig.lastUsedTime.toString()));
            assertThat(Set.copyOf((Collection<?>) read.get("ids")), equalTo(orig.identifiers));
        }
    }

    protected GetFeatureUsageResponse randomResponse() {
        final String featureName = randomAlphaOfLengthBetween(8, 24);
        final Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        final ZonedDateTime lastUsed = ZonedDateTime.ofInstant(now.minusSeconds(randomIntBetween(1, 10_000)), ZoneOffset.UTC);
        final String licenseLevel = randomAlphaOfLengthBetween(4, 12);
        final Set<String> identifiers = Set.copyOf(randomList(8, () -> randomAlphaOfLengthBetween(2, 8)));
        final List<GetFeatureUsageResponse.FeatureUsageInfo> usage = randomList(1, 5,
            () -> new GetFeatureUsageResponse.FeatureUsageInfo(featureName, lastUsed, licenseLevel, identifiers));
        return new GetFeatureUsageResponse(usage);
    }

    protected GetFeatureUsageResponse copyResponse(GetFeatureUsageResponse response, Version version) throws java.io.IOException {
        return copyWriteable(response, new NamedWriteableRegistry(List.of()), GetFeatureUsageResponse::new, version);
    }
}
