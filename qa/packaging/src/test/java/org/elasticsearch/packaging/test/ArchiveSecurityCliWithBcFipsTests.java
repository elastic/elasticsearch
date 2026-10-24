/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Platforms;
import org.junit.BeforeClass;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.junit.Assume.assumeTrue;

/**
 * A FIPS-enabled node keeps the BC FIPS jars in {@code lib}, which the security CLI tools load as their parent classpath. The
 * tools' own (shaded) Bouncy Castle in {@code lib/tools/security-cli} must therefore not share any class names with those jars, or
 * they fail at class initialisation (e.g. {@code NoSuchMethodError}). This verifies {@code certutil} and security auto-configuration
 * with the BC FIPS jars present.
 */
public class ArchiveSecurityCliWithBcFipsTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("archives only", distribution.isArchive());
    }

    public void test10InstallWithBcFipsJarsInLib() throws Exception {
        installation = installArchive(sh, distribution());
        verifyArchiveInstallation(installation, distribution());
        for (String jar : System.getProperty("tests.bcfips.jars").split(File.pathSeparator)) {
            Path source = Paths.get(jar);
            Files.copy(source, installation.lib.resolve(source.getFileName()));
        }
    }

    public void test20Certutil() throws Exception {
        Path out = installation.home.resolve("ca.zip");
        installation.executables().certutilTool.run("ca --pem --silent --out " + out);
        assertThat(out, fileExists());
    }

    public void test30AutoConfiguration() throws Exception {
        // auto-config requires that the archive owner and the process user be the same
        Platforms.onWindows(() -> sh.chown(installation.config, installation.getOwner()));
        FileUtils.assertPathsDoNotExist(installation.data);
        startElasticsearch();
        verifySecurityAutoConfigured(installation);
        stopElasticsearch();
    }
}
