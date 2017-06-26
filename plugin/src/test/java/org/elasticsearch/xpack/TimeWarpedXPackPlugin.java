/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.bouncycastle.operator.OperatorCreationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.elasticsearch.xpack.watcher.test.TimeWarpedWatcher;

import javax.security.auth.DestroyFailedException;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Clock;

public class TimeWarpedXPackPlugin extends XPackPlugin {

    // use a single clock across all nodes using this plugin, this lets keep it static
    private static final ClockMock clock = new ClockMock();

    public TimeWarpedXPackPlugin(Settings settings, Path configPath) throws IOException,
            DestroyFailedException, OperatorCreationException, GeneralSecurityException {
        super(settings, configPath);
        watcher = new TimeWarpedWatcher(settings);
    }

    @Override
    protected Clock getClock() {
        return clock;
    }

}
