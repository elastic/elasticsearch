/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class WatcherPluginTests extends ESTestCase {

    public void testValidAutoCreateIndex() {
        Watcher.validAutoCreateIndex(Settings.EMPTY, logger);
        Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", true).build(), logger);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", false).build(), logger));
        assertThat(exception.getMessage(), containsString("[.watches, .triggered_watches, .watcher-history-*]"));

        Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index",
                ".watches,.triggered_watches,.watcher-history*").build(), logger);
        Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", "*w*").build(), logger);
        Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".w*,.t*").build(), logger);

        exception = expectThrows(IllegalArgumentException.class,
                () -> Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".watches").build(), logger));
        assertThat(exception.getMessage(), containsString("[.watches, .triggered_watches, .watcher-history-*]"));

        exception = expectThrows(IllegalArgumentException.class,
                () -> Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".triggered_watch").build(), logger));
        assertThat(exception.getMessage(), containsString("[.watches, .triggered_watches, .watcher-history-*]"));

        exception = expectThrows(IllegalArgumentException.class,
                () -> Watcher.validAutoCreateIndex(Settings.builder().put("action.auto_create_index", ".watcher-history-*").build(),
                        logger));
        assertThat(exception.getMessage(), containsString("[.watches, .triggered_watches, .watcher-history-*]"));
    }

    public void testThreadPoolSize() {
        // old calculation was 5 * number of processors
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 1).build()), is(5));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 2).build()), is(10));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 4).build()), is(20));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 8).build()), is(40));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 9).build()), is(45));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 10).build()), is(50));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 16).build()), is(50));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 24).build()), is(50));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 50).build()), is(50));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 51).build()), is(51));
        assertThat(Watcher.getWatcherThreadPoolSize(Settings.builder().put("processors", 96).build()), is(96));
    }
}
