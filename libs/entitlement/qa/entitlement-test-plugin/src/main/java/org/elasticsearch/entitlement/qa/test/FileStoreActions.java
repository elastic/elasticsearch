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
import java.nio.file.Files;
import java.nio.file.attribute.FileStoreAttributeView;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.SERVER_ONLY;

@SuppressWarnings({ "unused" /* called via reflection */ })
class FileStoreActions {

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void checkGetFileStoreAttributeView() throws IOException {
        Files.getFileStore(FileCheckActions.readWriteFile()).getFileStoreAttributeView(FileStoreAttributeView.class);
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkGetAttribute() throws IOException {
        try {
            Files.getFileStore(FileCheckActions.readFile()).getAttribute("zfs:compression");
        } catch (UnsupportedOperationException e) {
            // It's OK if the attribute view is not available or it does not support reading the attribute
        }
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkGetBlockSize() throws IOException {
        Files.getFileStore(FileCheckActions.readWriteFile()).getBlockSize();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkGetTotalSpace() throws IOException {
        Files.getFileStore(FileCheckActions.readWriteFile()).getTotalSpace();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkGetUnallocatedSpace() throws IOException {
        Files.getFileStore(FileCheckActions.readWriteFile()).getUnallocatedSpace();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkGetUsableSpace() throws IOException {
        Files.getFileStore(FileCheckActions.readFile()).getUsableSpace();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkIsReadOnly() throws IOException {
        Files.getFileStore(FileCheckActions.readFile()).isReadOnly();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkName() throws IOException {
        Files.getFileStore(FileCheckActions.readFile()).name();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkType() throws IOException {
        Files.getFileStore(FileCheckActions.readFile()).type();
    }

    private FileStoreActions() {}
}
