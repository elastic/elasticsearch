/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Adds {@link TestDeprecationHeaderRestAction} for testing deprecation requests via HTTP.
 */
public class TestDeprecationPlugin extends Plugin implements ActionPlugin, SearchPlugin {

    @Override
    public List<RestHandler> getRestHandlers(RestHandlerParameters parameters) {
        return Collections.singletonList(new TestDeprecationHeaderRestAction(parameters.settings()));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1,
            TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2,
            TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING
        );
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(
            new QuerySpec<>(TestDeprecatedQueryBuilder.NAME, TestDeprecatedQueryBuilder::new, TestDeprecatedQueryBuilder::fromXContent)
        );
    }

}
