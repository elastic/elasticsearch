/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class AutoFollowMetadataTests extends AbstractChunkedSerializingTestCase<AutoFollowMetadata> {

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
        Map<String, AutoFollowMetadata.AutoFollowPattern> configs = Maps.newMapWithExpectedSize(numEntries);
        Map<String, List<String>> followedLeaderIndices = Maps.newMapWithExpectedSize(numEntries);
        Map<String, Map<String, String>> headers = Maps.newMapWithExpectedSize(numEntries);
        for (int i = 0; i < numEntries; i++) {
            List<String> leaderPatterns = Arrays.asList(generateRandomStringArray(4, 4, false));
            List<String> leaderExclusionPatterns = Arrays.asList(generateRandomStringArray(4, 4, false));
            AutoFollowMetadata.AutoFollowPattern autoFollowPattern = new AutoFollowMetadata.AutoFollowPattern(
                randomAlphaOfLength(4),
                leaderPatterns,
                leaderExclusionPatterns,
                randomAlphaOfLength(4),
                Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 4)).build(),
                true,
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                ByteSizeValue.ofBytes(randomNonNegativeLong()),
                ByteSizeValue.ofBytes(randomNonNegativeLong()),
                randomIntBetween(0, Integer.MAX_VALUE),
                ByteSizeValue.ofBytes(randomNonNegativeLong()),
                TimeValue.timeValueMillis(500),
                TimeValue.timeValueMillis(500)
            );
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
    protected AutoFollowMetadata mutateInstance(AutoFollowMetadata instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<AutoFollowMetadata> instanceReader() {
        return AutoFollowMetadata::new;
    }
}
