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

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

public class ClusterUpdateSettingsRequestTests extends ESTestCase {

    public void testFromToXContent() throws IOException {
        final ClusterUpdateSettingsRequest request = createTestItem();
        boolean humanReadable = randomBoolean();
        final XContentType xContentType = XContentType.JSON;
        BytesReference xContent = toShuffledXContentAndInsertRandomFields(request, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        XContentParser parser = createParser(xContentType.xContent(), xContent);
        ClusterUpdateSettingsRequest parsedRequest = ClusterUpdateSettingsRequest.fromXContent(parser);

        assertNull(parser.nextToken());

        Builder persistentBuilder = Settings.builder().put(request.persistentSettings());
        Builder parsedPersistentBuilder = Settings.builder().put(parsedRequest.persistentSettings());

        persistentBuilder.keys().forEach(k -> {
            assertThat(parsedPersistentBuilder.keys(), hasItem(equalTo(k)));
            assertThat(parsedPersistentBuilder.get(k), equalTo(persistentBuilder.get(k)));
        });

        Builder transientBuilder = Settings.builder().put(request.persistentSettings());
        Builder parsedTransientBuilder = Settings.builder().put(parsedRequest.persistentSettings());

        transientBuilder.keys().forEach(k -> {
            assertThat(parsedTransientBuilder.keys(), hasItem(equalTo(k)));
            assertThat(parsedTransientBuilder.get(k), equalTo(transientBuilder.get(k)));
        });
    }

    private static ClusterUpdateSettingsRequest createTestItem() {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        request.transientSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        return request;
    }
}
