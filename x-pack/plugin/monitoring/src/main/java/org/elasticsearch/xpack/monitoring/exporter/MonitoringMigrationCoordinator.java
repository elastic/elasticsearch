/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.exporter;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A shared coordination object for pausing execution of exporter installation tasks when a migration that involves them is in progress
 */
public class MonitoringMigrationCoordinator {

    // True value signals a migration is in progress
    private final AtomicBoolean migrationBlock;

    public MonitoringMigrationCoordinator() {
        this.migrationBlock = new AtomicBoolean(false);
    }

    public boolean tryBlockInstallationTasks() throws InterruptedException {
        return migrationBlock.compareAndSet(false, true);
    }

    public void unblockInstallationTasks() {
        migrationBlock.set(false);
    }

    public boolean canInstall() {
        return migrationBlock.get() == false;
    }
}
