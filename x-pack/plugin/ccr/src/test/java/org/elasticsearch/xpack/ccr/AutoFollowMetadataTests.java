/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class AutoFollowMetadataTests extends AbstractSerializingTestCase<AutoFollowMetadata> {

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return s -> true;
    }

    @Override
    protected AutoFollowMetadata doParseInstance(XContentParser parser) throws IOException {
        return AutoFollowMetadata.fromXContent(parser);
    }

    @Override
    protected AutoFollowMetadata createTestInstance() {
        int numEntries = randomIntBetween(0, 32);
        Map<String, AutoFollowMetadata.AutoFollowPattern> configs = new HashMap<>(numEntries);
        Map<String, List<String>> followedLeaderIndices = new HashMap<>(numEntries);
        Map<String, Map<String, String>> headers = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            List<String> leaderPatterns = Arrays.asList(generateRandomStringArray(4, 4, false));
            AutoFollowMetadata.AutoFollowPattern autoFollowPattern = new AutoFollowMetadata.AutoFollowPattern(
                randomAlphaOfLength(4),
                leaderPatterns,
                randomAlphaOfLength(4),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES),
                new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES),
                randomIntBetween(0, Integer.MAX_VALUE),
                new ByteSizeValue(randomNonNegativeLong()),
                TimeValue.timeValueMillis(500),
                TimeValue.timeValueMillis(500));
            configs.put(Integer.toString(i), autoFollowPattern);
            followedLeaderIndices.put(Integer.toString(i), Arrays.asList(generateRandomStringArray(4, 4, false)));
            if (randomBoolean()) {
                int numHeaderEntries = randomIntBetween(1, 16);
                Map<String, String> header = new HashMap<>();
                for (int j = 0; j < numHeaderEntries; j++) {
                    header.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
                }
                headers.put(Integer.toString(i), header);
            }
        }
        return new AutoFollowMetadata(configs, followedLeaderIndices, headers);
    }

    @Override
    protected Writeable.Reader<AutoFollowMetadata> instanceReader() {
        return AutoFollowMetadata::new;
    }
}
