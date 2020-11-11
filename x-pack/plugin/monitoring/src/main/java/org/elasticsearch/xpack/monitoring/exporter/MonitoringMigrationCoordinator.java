/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.exporter;

import java.util.concurrent.Semaphore;

import org.elasticsearch.common.unit.TimeValue;

/**
 * A shared coordination object for blocking execution of exporters when a migration that involves them is in progress
 */
public class MonitoringMigrationCoordinator {

    private final int MAX_PERMIT = 1;
    private final Semaphore migrationLock;

    public MonitoringMigrationCoordinator() {
        this.migrationLock = new Semaphore(MAX_PERMIT);
    }

    public boolean tryBlockInstallationTasks(TimeValue timeout) throws InterruptedException {
        return migrationLock.tryAcquire(timeout.duration(), timeout.timeUnit());
    }

    public void unblockInstallationTasks() {
        migrationLock.release();
    }

    public boolean canInstall() {
        return MAX_PERMIT == migrationLock.availablePermits();
    }
}
