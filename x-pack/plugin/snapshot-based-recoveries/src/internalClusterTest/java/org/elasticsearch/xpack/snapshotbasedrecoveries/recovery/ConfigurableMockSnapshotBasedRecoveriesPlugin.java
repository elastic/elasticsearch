/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries.recovery;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.xpack.snapshotbasedrecoveries.SnapshotBasedRecoveriesPlugin;

import java.util.concurrent.atomic.AtomicBoolean;

public class ConfigurableMockSnapshotBasedRecoveriesPlugin extends SnapshotBasedRecoveriesPlugin {
    private static final AtomicBoolean recoveryFromSnapshotAllowed = new AtomicBoolean(true);

    public ConfigurableMockSnapshotBasedRecoveriesPlugin(Settings settings) {
        super(settings);
    }

    @Override
    public boolean isLicenseEnabled() {
        return recoveryFromSnapshotAllowed.get();
    }

    public static void denyRecoveryFromSnapshot(CheckedRunnable<Exception> runnable) throws Exception {
        assert recoveryFromSnapshotAllowed.get();

        recoveryFromSnapshotAllowed.set(false);
        try {
            runnable.run();
        } finally {
            recoveryFromSnapshotAllowed.set(true);
        }
    }
}
