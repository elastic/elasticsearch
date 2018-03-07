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

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;

public class UpdateSettingsRequestTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        doFromXContentTestWithSettingsField(false);
    }

    public void testFromXContentWithSettingsField() throws IOException {
        doFromXContentTestWithSettingsField(true);
    }

    private void doFromXContentTestWithSettingsField(boolean addSettingsField) throws IOException {
        final UpdateSettingsRequest request = createTestItem();
        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());

        BytesReference bytesRef;
        if (addSettingsField) {
            UpdateSettingsRequest requestWithEnclosingSettings = new UpdateSettingsRequest(request.settings()) {
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.startObject();
                    builder.startObject("settings");
                    this.settings().toXContent(builder, params);
                    builder.endObject();
                    builder.endObject();
                    return builder;
                }
            };
            bytesRef = toShuffledXContent(requestWithEnclosingSettings, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        } else {
            bytesRef = toShuffledXContent(request, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        }

        XContentParser parser = createParser(xContentType.xContent(), bytesRef);
        UpdateSettingsRequest parsedRequest = new UpdateSettingsRequest().fromXContent(parser);

        assertNull(parser.nextToken());
        assertThat(parsedRequest.settings(), equalTo(request.settings()));
    }

    private static UpdateSettingsRequest createTestItem() {
        return new UpdateSettingsRequest().settings(randomSettings(0, 2));
    }

    public static Settings randomSettings(int min, int max) {
        int num = randomIntBetween(min, max);
        Builder builder = Settings.builder();
        for (int i = 0; i < num; i++) {
            builder.put(randomAlphaOfLength(5), randomAlphaOfLengthBetween(2, 10));
        }
        return builder.build();
    }

}
