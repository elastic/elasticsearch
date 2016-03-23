/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;
import java.nio.file.Path;

public class XPackExtensionsServiceTests extends ESTestCase {
    public void testExistingPluginMissingDescriptor() throws Exception {
        Path extensionsDir = createTempDir();
        Files.createDirectory(extensionsDir.resolve("extension-missing-descriptor"));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            XPackExtensionsService.getExtensionBundles(extensionsDir);
        });
        assertTrue(e.getMessage(),
                e.getMessage().contains("Could not load extension descriptor for existing extension"));
    }
}
