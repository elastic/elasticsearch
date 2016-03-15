/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class WatcherPluginTests extends ESTestCase {

    public void testValidAutoCreateIndex() {
        Watcher.validAutoCreateIndex(Settings.EMPTY);
        Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", true).build());
        try {
            Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", false).build());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[.watches,.triggered_watches,.watcher-history*]"));
        }
        Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index",
                ".watches,.triggered_watches,.watcher-history*").build());
        Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", "*w*").build());
        Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".w*,.t*").build());
        try {
            Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".watches").build());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[.watches,.triggered_watches,.watcher-history*]"));
        }
        try {
            Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".triggered_watch").build());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[.watches,.triggered_watches,.watcher-history*]"));
        }
        try {
            Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".watcher-history*").build());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[.watches,.triggered_watches,.watcher-history*]"));
        }
    }

}
