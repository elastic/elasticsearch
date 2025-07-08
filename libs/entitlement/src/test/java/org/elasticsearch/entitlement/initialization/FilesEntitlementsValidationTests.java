/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.PathLookupImpl;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.Scope;
import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

public class FilesEntitlementsValidationTests extends ESTestCase {

    private static PathLookup TEST_PATH_LOOKUP;

    private static Path TEST_CONFIG_DIR;

    private static Path TEST_PLUGINS_DIR;
    private static Path TEST_LIBS_DIR;

    @BeforeClass
    public static void beforeClass() {
        try {
            Path testBaseDir = createTempDir().toAbsolutePath();
            TEST_CONFIG_DIR = testBaseDir.resolve("config");
            TEST_PLUGINS_DIR = testBaseDir.resolve("plugins");
            TEST_LIBS_DIR = testBaseDir.resolve("libs");

            TEST_PATH_LOOKUP = new PathLookupImpl(
                testBaseDir.resolve("user/home"),
                TEST_CONFIG_DIR,
                new Path[] { testBaseDir.resolve("data1"), testBaseDir.resolve("data2") },
                new Path[] { testBaseDir.resolve("shared1"), testBaseDir.resolve("shared2") },
                TEST_LIBS_DIR,
                testBaseDir.resolve("modules"),
                TEST_PLUGINS_DIR,
                testBaseDir.resolve("logs"),
                testBaseDir.resolve("temp"),
                null,
                Settings.EMPTY::getValues
            );
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void testValidationPass() {
        var policy = new Policy(
            "plugin",
            List.of(
                new Scope(
                    "module1",
                    List.of(
                        new FilesEntitlement(List.of(FilesEntitlement.FileData.ofPath(TEST_CONFIG_DIR, FilesEntitlement.Mode.READ))),
                        new CreateClassLoaderEntitlement()
                    )
                )
            )
        );
        FilesEntitlementsValidation.validate(Map.of("plugin", policy), TEST_PATH_LOOKUP);
    }

    public void testValidationFailForRead() {
        var policy = new Policy(
            "plugin",
            List.of(
                new Scope(
                    "module2",
                    List.of(
                        new FilesEntitlement(List.of(FilesEntitlement.FileData.ofPath(TEST_PLUGINS_DIR, FilesEntitlement.Mode.READ))),
                        new CreateClassLoaderEntitlement()
                    )
                )
            )
        );

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> FilesEntitlementsValidation.validate(Map.of("plugin", policy), TEST_PATH_LOOKUP)
        );
        assertThat(
            ex.getMessage(),
            both(startsWith("policy for module [module2] in [plugin] has an invalid file entitlement")).and(
                endsWith("is forbidden for mode [READ].")
            )
        );

        // check fails for mode READ_WRITE too
        var policy2 = new Policy(
            "plugin",
            List.of(
                new Scope(
                    "module1",
                    List.of(
                        new FilesEntitlement(List.of(FilesEntitlement.FileData.ofPath(TEST_LIBS_DIR, FilesEntitlement.Mode.READ_WRITE))),
                        new CreateClassLoaderEntitlement()
                    )
                )
            )
        );

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> FilesEntitlementsValidation.validate(Map.of("plugin2", policy2), TEST_PATH_LOOKUP)
        );
        assertThat(
            ex.getMessage(),
            both(startsWith("policy for module [module1] in [plugin2] has an invalid file entitlement")).and(
                endsWith("is forbidden for mode [READ].")
            )
        );
    }

    public void testValidationFailForWrite() {
        var policy = new Policy(
            "plugin",
            List.of(
                new Scope(
                    "module1",
                    List.of(
                        new FilesEntitlement(List.of(FilesEntitlement.FileData.ofPath(TEST_CONFIG_DIR, FilesEntitlement.Mode.READ_WRITE))),
                        new CreateClassLoaderEntitlement()
                    )
                )
            )
        );

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> FilesEntitlementsValidation.validate(Map.of("plugin", policy), TEST_PATH_LOOKUP)
        );
        assertThat(
            ex.getMessage(),
            both(startsWith("policy for module [module1] in [plugin] has an invalid file entitlement")).and(
                endsWith("is forbidden for mode [READ_WRITE].")
            )
        );
    }
}
