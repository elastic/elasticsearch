/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

/**
 * Server utilities that delegate their entitlement checks to the plugin that uses them. The files they access are granted to
 * the plugin, not to server, so they are only accessible if the checks use the entitlements of the plugin.
 */
class DelegationActions {

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedNoOp = true)
    static boolean fileWatcherCreatedByPlugin() throws IOException {
        AtomicBoolean initialized = new AtomicBoolean();
        FileWatcher watcher = new FileWatcher(FileCheckActions.readFile());
        watcher.addListener(new FileChangesListener() {
            @Override
            public void onFileInit(Path file) {
                initialized.set(true);
            }
        });
        watcher.init();
        return initialized.get();
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IllegalArgumentException.class)
    static void analysisFileReadByPlugin(Environment environment) throws IOException {
        try (Reader reader = Analysis.getReaderFromFile(environment, FileCheckActions.readFile().toString(), "delegation_test")) {
            reader.read();
        }
    }

    private DelegationActions() {}
}
