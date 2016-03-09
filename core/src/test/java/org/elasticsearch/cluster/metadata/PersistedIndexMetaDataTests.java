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

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit level tests for the {@link PersistedIndexMetaData} class.
 */
public class PersistedIndexMetaDataTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final int numIterations = 5;
        for (int i = 0; i < numIterations; i++) {
            final String indexName = "idxMetaTest";
            final IndexMetaData indexMetaData = IndexMetaDataTests.randomIndexMetadata(indexName);
            final String clusterUUID = Strings.randomBase64UUID();
            final PersistedIndexMetaData persistedIndex = new PersistedIndexMetaData(indexMetaData, clusterUUID);
            final PersistedIndexMetaData fromStream = writeThenRead(persistedIndex);
            assertThat(fromStream, equalTo(persistedIndex));
        }
    }

    private static PersistedIndexMetaData writeThenRead(final PersistedIndexMetaData original) throws IOException {
        XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
        xContentBuilder.startObject();
        original.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        final byte[] bytes = xContentBuilder.bytes().toBytes();
        try (XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes)) {
            return PersistedIndexMetaData.PROTOTYPE.fromXContent(parser, ParseFieldMatcher.EMPTY);
        }
    }
}
