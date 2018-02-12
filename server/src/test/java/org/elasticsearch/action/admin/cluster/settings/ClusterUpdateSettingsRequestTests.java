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

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;

public class ClusterUpdateSettingsRequestTests extends ESTestCase {

    @Test
    public void fromToXContent() throws IOException {
        final ClusterUpdateSettingsRequest request = createTestItem();
        boolean humanReadable = randomBoolean();
        final XContentType xContentType = XContentType.JSON;
        BytesReference xContent = toShuffledXContent(request, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        XContentParser parser = createParser(xContentType.xContent(), xContent);
        ClusterUpdateSettingsRequest parsedRequest = ClusterUpdateSettingsRequest.fromXContent(parser);
        assertNull(parser.nextToken());

        assertThat(parsedRequest.persistentSettings(), equalTo(request.persistentSettings()));
        assertThat(parsedRequest.transientSettings(), equalTo(request.transientSettings()));
    }

    private static ClusterUpdateSettingsRequest createTestItem() {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        request.transientSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        return request;
    }
}
