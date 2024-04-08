/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.Predicate;

public class RestClusterGetSettingsResponseTests extends AbstractXContentTestCase<RestClusterGetSettingsResponse> {

    @Override
    protected RestClusterGetSettingsResponse doParseInstance(XContentParser parser) throws IOException {
        return RestClusterGetSettingsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected RestClusterGetSettingsResponse createTestInstance() {
        Settings persistentSettings = ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2);
        Settings transientSettings = ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2);
        Settings defaultSettings = randomBoolean() ? ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2) : Settings.EMPTY;
        return new RestClusterGetSettingsResponse(persistentSettings, transientSettings, defaultSettings);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p -> p.startsWith(RestClusterGetSettingsResponse.TRANSIENT_FIELD)
            || p.startsWith(RestClusterGetSettingsResponse.PERSISTENT_FIELD)
            || p.startsWith(RestClusterGetSettingsResponse.DEFAULTS_FIELD);
    }
}
