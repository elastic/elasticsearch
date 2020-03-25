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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class UpdateSettingsRequestTests extends AbstractXContentTestCase<UpdateSettingsRequest> {

    private final boolean enclosedSettings = randomBoolean();

    @Override
    protected UpdateSettingsRequest createTestInstance() {
        UpdateSettingsRequest testRequest = UpdateSettingsRequestSerializationTests.createTestItem();
        if (enclosedSettings) {
            UpdateSettingsRequest requestWithEnclosingSettings = new UpdateSettingsRequest(testRequest.settings()) {
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.startObject();
                    builder.startObject("settings");
                    this.settings().toXContent(builder, params);
                    builder.endObject();
                    builder.endObject();
                    return builder;
                }
            };
            return requestWithEnclosingSettings;
        }
        return testRequest;
    }

    @Override
    protected UpdateSettingsRequest doParseInstance(XContentParser parser) throws IOException {
        return new UpdateSettingsRequest().fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        // if the settings are enclose as a "settings" object
        // then all other top-level elements will be ignored during the parsing
        return enclosedSettings;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        if (enclosedSettings) {
            return field -> field.startsWith("settings");
        }
        return field -> true;
    }

    @Override
    protected void assertEqualInstances(UpdateSettingsRequest expectedInstance, UpdateSettingsRequest newInstance) {
        // here only the settings should be tested, as this test covers explicitly only the XContent parsing
        // the rest of the request fields are tested by the SerializingTests
        super.assertEqualInstances(new UpdateSettingsRequest(expectedInstance.settings()),
                new UpdateSettingsRequest(newInstance.settings()));
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        // if enclosedSettings are used, disable the XContentEquivalence check as the
        // parsed.toXContent is not equivalent to the test instance
        return !enclosedSettings;
    }

}
