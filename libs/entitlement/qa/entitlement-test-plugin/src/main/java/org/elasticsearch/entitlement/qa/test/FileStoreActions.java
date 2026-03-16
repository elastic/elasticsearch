/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.entitlement.qa.entitled.EntitledActions;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileStoreAttributeView;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.SERVER_ONLY;

@SuppressWarnings({ "unused" /* called via reflection */ })
class FileStoreActions {

    private static FileStore fileStore() throws IOException {
        return EntitledActions.getFileStore(FileCheckActions.readWriteFile());
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, isExpectedDefaultNull = true)
    static FileStoreAttributeView checkGetFileStoreAttributeView() throws IOException {
        return fileStore().getFileStoreAttributeView(FileStoreAttributeView.class);
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY, expectedExceptionIfDenied = IOException.class)
    static void checkGetAttribute() throws IOException {
        try {
            fileStore().getAttribute("zfs:compression");
        } catch (UnsupportedOperationException e) {
            // It's OK if the attribute view is not available or it does not support reading the attribute
        }
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY, expectedExceptionIfDenied = IOException.class)
    static void checkGetBlockSize() throws IOException {
        fileStore().getBlockSize();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY, expectedExceptionIfDenied = IOException.class)
    static void checkGetTotalSpace() throws IOException {
        fileStore().getTotalSpace();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY, expectedExceptionIfDenied = IOException.class)
    static void checkGetUnallocatedSpace() throws IOException {
        fileStore().getUnallocatedSpace();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY, expectedExceptionIfDenied = IOException.class)
    static void checkGetUsableSpace() throws IOException {
        fileStore().getUsableSpace();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY, expectedDefaultIfDenied = "true", expectedDefaultType = boolean.class)
    static boolean checkIsReadOnly() throws IOException {
        return fileStore().isReadOnly();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkName() throws IOException {
        fileStore().name();
    }

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void checkType() throws IOException {
        fileStore().type();
    }

    private FileStoreActions() {}
}
