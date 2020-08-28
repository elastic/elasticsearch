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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class ClusterGetSettingsResponseTests extends AbstractXContentTestCase<ClusterGetSettingsResponse> {

    @Override
    protected ClusterGetSettingsResponse doParseInstance(XContentParser parser) throws IOException {
        return ClusterGetSettingsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected ClusterGetSettingsResponse createTestInstance() {
        Settings persistentSettings = ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2);
        Settings transientSettings = ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2);
        Settings defaultSettings = randomBoolean() ?
            ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2): Settings.EMPTY;
        return new ClusterGetSettingsResponse(persistentSettings, transientSettings, defaultSettings);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p ->
            p.startsWith(ClusterGetSettingsResponse.TRANSIENT_FIELD) ||
                p.startsWith(ClusterGetSettingsResponse.PERSISTENT_FIELD) ||
                p.startsWith(ClusterGetSettingsResponse.DEFAULTS_FIELD);
    }
}
