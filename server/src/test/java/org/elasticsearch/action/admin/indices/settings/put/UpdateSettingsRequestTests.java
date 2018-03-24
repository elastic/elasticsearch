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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Predicate;

public class UpdateSettingsRequestTests extends AbstractStreamableXContentTestCase<UpdateSettingsRequest> {

    @Override
    protected UpdateSettingsRequest doParseInstance(XContentParser parser) throws IOException {
        return new UpdateSettingsRequest().fromXContent(parser);
    }

    @Override
    protected UpdateSettingsRequest mutateInstance(UpdateSettingsRequest request) {
        return new UpdateSettingsRequest(mutateSettings(request.settings()), request.indices());
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // do not insert any fields into the request body, as every inserted field will be interpreted as a new setting
        return p -> true;
    }

    @Override
    protected UpdateSettingsRequest createTestInstance() {
        return randomBoolean()
                ? new UpdateSettingsRequest(randomSettings(0, 2))
                : new UpdateSettingsRequest(randomSettings(0, 2), randomIndicesNames(0, 2));
    }

    @Override
    protected UpdateSettingsRequest createBlankInstance() {
        return new UpdateSettingsRequest();
    }

    private static Settings mutateSettings(Settings settings) {
        if (settings.isEmpty()) {
            return randomSettings(1, 5);
        }
        Set<String> allKeys = settings.keySet();
        List<String> keysToBeModified = randomSubsetOf(randomIntBetween(1, allKeys.size()), allKeys);
        Builder builder = Settings.builder();
        for (String key : allKeys) {
            String value = settings.get(key);
            if (keysToBeModified.contains(key)) {
                value += randomAlphaOfLengthBetween(2, 5);
            }
            builder.put(key, value);
        }
        return builder.build();
    }

    private static Settings randomSettings(int min, int max) {
        int num = randomIntBetween(min, max);
        Builder builder = Settings.builder();
        for (int i = 0; i < num; i++) {
            int keyDepth = randomIntBetween(1, 5);
            StringJoiner keyJoiner = new StringJoiner(".", "", "");
            for (int d = 0; d < keyDepth; d++) {
                keyJoiner.add(randomAlphaOfLengthBetween(3, 5));
            }
            builder.put(keyJoiner.toString(), randomAlphaOfLengthBetween(2, 5));
        }
        return builder.build();
    }

    private static String[] randomIndicesNames(int minIndicesNum, int maxIndicesNum) {
        int numIndices = randomIntBetween(minIndicesNum, maxIndicesNum);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = "index-" + randomAlphaOfLengthBetween(2, 5).toLowerCase(Locale.ROOT);
        }
        return indices;
    }

}
