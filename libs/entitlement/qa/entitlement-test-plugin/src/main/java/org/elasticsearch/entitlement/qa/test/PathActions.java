/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.WatchEvent;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressWarnings({ "unused" /* called via reflection */, "rawtypes" })
class PathActions {

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkToRealPath() throws IOException {
        FileCheckActions.readFile().toRealPath();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkToRealPathNoFollow() throws IOException {
        FileCheckActions.readFile().toRealPath(LinkOption.NOFOLLOW_LINKS);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkRegister() throws IOException {
        try (var watchService = FileSystems.getDefault().newWatchService()) {
            FileCheckActions.readFile().register(watchService, new WatchEvent.Kind[0]);
        } catch (IllegalArgumentException e) {
            // intentionally no events registered
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkRegisterWithModifiers() throws IOException {
        try (var watchService = FileSystems.getDefault().newWatchService()) {
            FileCheckActions.readFile().register(watchService, new WatchEvent.Kind[0], new WatchEvent.Modifier[0]);
        } catch (IllegalArgumentException e) {
            // intentionally no events registered
        }
    }
}
