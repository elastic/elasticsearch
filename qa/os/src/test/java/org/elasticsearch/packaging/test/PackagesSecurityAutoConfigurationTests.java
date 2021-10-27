/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Packages;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.installPackage;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeTrue;

public class PackagesSecurityAutoConfigurationTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("rpm or deb", distribution.isPackage());
    }

    public void test10SecurityAutoConfiguredOnPackageInstall() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution(), successfulAutoConfiguration());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        verifySecurityAutoConfigured(installation);
        assertNotNull(installation.getElasticPassword());
    }

    public void test20SecurityNotAutoConfiguredOnReInstallation() throws Exception {
        // we are testing force upgrading in the current version
        // In such a case, security remains configured from the initial installation, we don't run it again.
        Optional<String> autoConfigDirName = getAutoConfigDirName(installation);
        installation = Packages.forceUpgradePackage(sh, distribution);
        assertInstalled(distribution);
        verifyPackageInstallation(installation, distribution, sh);
        verifySecurityAutoConfigured(installation);
        // Since we did not auto-configure the second time, the directory name should be the same
        assertThat(autoConfigDirName.isPresent(), is(true));
        assertThat(getAutoConfigDirName(installation).isPresent(), is(true));
        assertThat(getAutoConfigDirName(installation).get(), equalTo(autoConfigDirName.get()));
    }

    public void test30SecurityNotAutoConfiguredWhenExistingDataDir() throws Exception {
        // This is a contrived example for packages where in a new installation, there is an
        // existing data directory but the rest of the package tracked config files were removed
        final Path dataPath = installation.data;
        cleanup();
        Files.createDirectory(dataPath);
        append(dataPath.resolve("foo"), "some data");
        installation = installPackage(sh, distribution(), existingSecurityConfiguration());
        verifySecurityNotAutoConfigured(installation);
    }

    public void test40SecurityNotAutoConfiguredWhenExistingKeystoreUnknownPassword() throws Exception {
        // This is a contrived example for packages where in a new installation, there is an
        // existing elasticsearch.keystore file within $ES_PATH_CONF and it's password-protected
        final Installation.Executables bin = installation.executables();
        bin.keystoreTool.run("passwd", "some_password\nsome_password\n");
        final Path tempDir = createTempDir("existing-keystore-config");
        final Path confPath = installation.config;
        Files.copy(
            confPath.resolve("elasticsearch.keystore"),
            tempDir.resolve("elasticsearch.keystore"),
            StandardCopyOption.COPY_ATTRIBUTES
        );
        cleanup();
        Files.createDirectory(confPath);
        Files.copy(
            tempDir.resolve("elasticsearch.keystore"),
            confPath.resolve("elasticsearch.keystore"),
            StandardCopyOption.COPY_ATTRIBUTES
        );
        installation = installPackage(sh, distribution(), errorOutput());
        List<String> configLines = Files.readAllLines(installation.config("elasticsearch.yml"));
        assertThat(configLines, not(hasItem("# have been automatically generated in order to configure Security.               #")));
    }

    private Predicate<String> successfulAutoConfiguration() {
        Predicate<String> p1 = output -> output.contains("Authentication and authorization are enabled.");
        Predicate<String> p2 = output -> output.contains("TLS for the transport and HTTP layers is enabled and configured.");
        Predicate<String> p3 = output -> output.contains("The generated password for the elastic built-in superuser is :");
        return p1.and(p2).and(p3);
    }

    private Predicate<String> existingSecurityConfiguration() {
        return output -> output.contains("Skipping auto-configuration because security features appear to be already configured.");
    }

    private Predicate<String> errorOutput() {
        Predicate<String> p1 = output -> output.contains("Failed to auto-configure security features.");
        Predicate<String> p2 = output -> output.contains("However, authentication and authorization are still enabled.");
        Predicate<String> p3 = output -> output.contains("You can reset the password of the elastic built-in superuser with");
        Predicate<String> p4 = output -> output.contains("/usr/share/elasticsearch/bin/elasticsearch-reset-password -u elastic");
        return p1.and(p2).and(p3).and(p4);
    }

}
