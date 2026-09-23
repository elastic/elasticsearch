/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class RestTestDataSourceConnectionActionTests extends ESTestCase {

    public void testGetFilteredFieldsEmpty() {
        RestTestDataSourceConnectionAction action = new RestTestDataSourceConnectionAction(Set.of());
        assertThat(action.getFilteredFields(), empty());
    }

    public void testGetFilteredFieldsMapsToSettingsPaths() {
        RestTestDataSourceConnectionAction action = new RestTestDataSourceConnectionAction(
            Set.of("access_key", "secret_key", "session_token")
        );
        assertThat(action.getFilteredFields(), containsInAnyOrder("settings.access_key", "settings.secret_key", "settings.session_token"));
    }

    public void testGetFilteredFieldsGcs() {
        RestTestDataSourceConnectionAction action = new RestTestDataSourceConnectionAction(Set.of("credentials", "access_token"));
        assertThat(action.getFilteredFields(), containsInAnyOrder("settings.credentials", "settings.access_token"));
    }

    public void testGetFilteredFieldsAzure() {
        RestTestDataSourceConnectionAction action = new RestTestDataSourceConnectionAction(Set.of("connection_string", "key", "sas_token"));
        assertThat(action.getFilteredFields(), containsInAnyOrder("settings.connection_string", "settings.key", "settings.sas_token"));
    }
}
