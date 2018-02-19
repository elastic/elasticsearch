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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.CoreMatchers.equalTo;

public class ClusterUpdateSettingsResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        final ClusterUpdateSettingsResponse response = createTestItem();
        boolean humanReadable = randomBoolean();
        final XContentType xContentType = XContentType.JSON;

        BytesReference originalBytes = toShuffledXContent(response, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xContentType, originalBytes, p -> p.startsWith("transient") || p.startsWith("persistent"),
                    random());
        } else {
            mutated = originalBytes;
        }

        XContentParser parser = createParser(xContentType.xContent(), mutated);
        ClusterUpdateSettingsResponse parsedResponse = ClusterUpdateSettingsResponse.fromXContent(parser);

        assertNull(parser.nextToken());
        assertThat(parsedResponse.isAcknowledged(), equalTo(response.isAcknowledged()));
        assertThat(response.transientSettings, equalTo(response.transientSettings));
        assertThat(response.persistentSettings, equalTo(response.persistentSettings));
    }

    public static Settings randomClusterSettings(int min, int max) {
        int num = randomIntBetween(min, max);
        Builder builder = Settings.builder();
        for (int i = 0; i < num; i++) {
            Setting<?> setting = randomFrom(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            builder.put(setting.getKey(), randomAlphaOfLengthBetween(2, 10));
        }
        return builder.build();
    }

    private static ClusterUpdateSettingsResponse createTestItem() {
        return new ClusterUpdateSettingsResponse(randomBoolean(), randomClusterSettings(0, 2), randomClusterSettings(0, 2));
    }
}
