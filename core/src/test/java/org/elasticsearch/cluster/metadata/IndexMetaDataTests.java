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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for the {@link IndexMetaData} class.
 * TODO: flesh out with more tests over time
 */
public class IndexMetaDataTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final int numIterations = 5;
        for (int i = 0; i < numIterations; i++) {
            final String indexName = "idxMetaTest";
            final IndexMetaData indexMetaData = randomIndexMetadata(indexName);
            final IndexMetaData fromStream = writeThenRead(indexMetaData);
            assertThat(fromStream, equalTo(indexMetaData));
        }
    }

    private static IndexMetaData writeThenRead(final IndexMetaData indexMetaData) throws Exception {
        XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
        xContentBuilder.startObject();
        indexMetaData.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        final byte[] bytes = xContentBuilder.bytes().toBytes();
        try (XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes)) {
            return IndexMetaData.PROTO.fromXContent(parser, ParseFieldMatcher.EMPTY);
        }
    }

    public static IndexMetaData randomIndexMetadata(final String indexName) {
        IndexMetaData.Builder builder = IndexMetaData.builder(indexName);
        builder.creationDate(randomPositiveLong());
        builder.version((long) randomIntBetween(0, 20000));
        builder.state(randomBoolean() ? IndexMetaData.State.OPEN : IndexMetaData.State.CLOSE);
        Settings.Builder settingsBuilder = Settings.settingsBuilder();
        settingsBuilder.put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT);
        settingsBuilder.put(IndexMetaData.SETTING_VERSION_UPGRADED, Version.CURRENT);
        settingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 20));
        settingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 20));
        builder.settings(settingsBuilder);
        // TODO: add more randomization for mappings, aliases, customs, and activeAllocationIds
        return builder.build();
    }
}
