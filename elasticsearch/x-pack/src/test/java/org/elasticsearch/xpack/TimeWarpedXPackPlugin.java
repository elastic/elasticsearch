/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.support.clock.Clock;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.elasticsearch.xpack.support.clock.ClockModule;
import org.elasticsearch.xpack.watcher.test.TimeWarpedWatcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TimeWarpedXPackPlugin extends XPackPlugin {

    public TimeWarpedXPackPlugin(Settings settings) {
        super(settings);
        watcher = new TimeWarpedWatcher(settings);
    }

    @Override
    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>(super.nodeModules());
        for (int i = 0; i < modules.size(); ++i) {
            Module module = modules.get(i);
            if (module instanceof ClockModule) {
                // replacing the clock module so we'll be able
                // to control time in tests
                modules.set(i, new MockClockModule());
            }
        }
        return modules;
    }

    public static class MockClockModule extends ClockModule {
        @Override
        protected void configure() {
            bind(ClockMock.class).asEagerSingleton();
            bind(Clock.class).to(ClockMock.class);
        }
    }
}
