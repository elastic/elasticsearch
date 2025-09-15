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
import java.nio.file.attribute.BasicFileAttributes;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

class VersionSpecificNioFileSystemActions {
    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkReadAttributesIfExists() throws IOException {
        var fs = FileSystems.getDefault().provider();
        fs.readAttributesIfExists(FileCheckActions.readFile(), BasicFileAttributes.class);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkExists() {
        var fs = FileSystems.getDefault().provider();
        fs.exists(FileCheckActions.readFile());
    }
}
