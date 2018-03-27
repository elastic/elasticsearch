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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.Matchers.equalTo;

public class UpdateSettingsRequestTests extends ESTestCase {

    public void testXContent() throws IOException {
        doToFromXContentWithEnclosingSettingsField(false);
    }

    // test that enclosing the setting in "settings" will be correctly parsed
    public void testXContentWithEnclosingSettingsField() throws IOException {
        doToFromXContentWithEnclosingSettingsField(true);
    }

    private void doToFromXContentWithEnclosingSettingsField(boolean addSettingsField) throws IOException {
        final UpdateSettingsRequest request = UpdateSettingsRequestStreamableTests.createTestItem();
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
            BytesReference originalBytes = toShuffledXContent(requestWithEnclosingSettings, xContentType, ToXContent.EMPTY_PARAMS,
                    humanReadable);
            if (randomBoolean()) {
                Predicate<String> excludeFilter = (s) -> s.startsWith("settings");
                bytesRef = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
            } else {
                bytesRef = originalBytes;
            }
        } else {
            bytesRef = toShuffledXContent(request, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        }

        XContentParser parser = createParser(xContentType.xContent(), bytesRef);
        UpdateSettingsRequest parsedRequest = new UpdateSettingsRequest().fromXContent(parser);

        assertNull(parser.nextToken());
        assertThat(parsedRequest.settings(), equalTo(request.settings()));
    }

}
