/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Packages;
import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.installPackage;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.hamcrest.Matchers.containsString;
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

    public void test50ReconfigureAndEnroll() throws Exception {
        cleanup();
        assertRemoved(distribution());
        installation = installPackage(sh, distribution(), successfulAutoConfiguration());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        verifySecurityAutoConfigured(installation);
        assertNotNull(installation.getElasticPassword());
        // We cannot run two packaged installations simultaneously here so that we can test that the second node enrolls successfully
        // We trigger with an invalid enrollment token, to verify that we removed the existing auto-configuration
        Shell.Result result = installation.executables().nodeReconfigureTool.run("--enrollment-token thisisinvalid", "y", true);
        assertThat(result.exitCode, equalTo(ExitCodes.DATA_ERROR)); // invalid enrollment token
        verifySecurityNotAutoConfigured(installation);
    }

    public void test60ReconfigureWithoutEnrollmentToken() throws Exception {
        cleanup();
        assertRemoved(distribution());
        installation = installPackage(sh, distribution(), successfulAutoConfiguration());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        verifySecurityAutoConfigured(installation);
        assertNotNull(installation.getElasticPassword());
        Shell.Result result = installation.executables().nodeReconfigureTool.run("", null, true);
        assertThat(result.exitCode, equalTo(ExitCodes.USAGE)); // missing enrollment token
        // we fail on command invocation so we don't even try to remove autoconfiguration
        verifySecurityAutoConfigured(installation);
    }

    // The following could very well be unit tests but the way we delete files doesn't play well with jimfs

    public void test70ReconfigureFailsWhenTlsAutoConfDirMissing() throws Exception {
        cleanup();
        assertRemoved(distribution());
        installation = installPackage(sh, distribution(), successfulAutoConfiguration());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        verifySecurityAutoConfigured(installation);
        assertNotNull(installation.getElasticPassword());

        Optional<String> autoConfigDirName = getAutoConfigDirName(installation);
        // Move instead of delete because Files.deleteIfExists bails on non empty dirs
        Files.move(installation.config(autoConfigDirName.get()), installation.config("temp-autoconf-dir"));
        Shell.Result result = installation.executables().nodeReconfigureTool.run("--enrollment-token a-token", "y", true);
        assertThat(result.exitCode, equalTo(ExitCodes.USAGE)); //
    }

    public void test71ReconfigureFailsWhenKeyStorePasswordWrong() throws Exception {
        cleanup();
        assertRemoved(distribution());
        installation = installPackage(sh, distribution(), successfulAutoConfiguration());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        verifySecurityAutoConfigured(installation);
        assertNotNull(installation.getElasticPassword());
        Shell.Result changePassword = installation.executables().keystoreTool.run("passwd", "some-password\nsome-password\n");
        assertThat(changePassword.exitCode, equalTo(0));
        Shell.Result result = installation.executables().nodeReconfigureTool.run(
            "--enrollment-token a-token",
            "y" + "\n" + "some-wrong-password",
            true
        );
        assertThat(result.exitCode, equalTo(ExitCodes.IO_ERROR)); //
        assertThat(result.stderr, containsString("Error was: Provided keystore password was incorrect"));
    }

    public void test71ReconfigureFailsWhenKeyStoreDoesNotContainExpectedSettings() throws Exception {
        cleanup();
        assertRemoved(distribution());
        installation = installPackage(sh, distribution(), successfulAutoConfiguration());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        verifySecurityAutoConfigured(installation);
        assertNotNull(installation.getElasticPassword());
        Shell.Result removeSetting = installation.executables().keystoreTool.run(
            "remove xpack.security.transport.ssl.keystore.secure_password"
        );
        assertThat(removeSetting.exitCode, equalTo(0));
        Shell.Result result = installation.executables().nodeReconfigureTool.run("--enrollment-token a-token", "y", true);
        assertThat(result.exitCode, equalTo(ExitCodes.IO_ERROR));
        assertThat(
            result.stderr,
            containsString(
                "elasticsearch.keystore did not contain expected setting [xpack.security.transport.ssl.keystore.secure_password]."
            )
        );
    }

    public void test72ReconfigureFailsWhenConfigurationDoesNotContainSecurityAutoConfig() throws Exception {
        cleanup();
        assertRemoved(distribution());
        installation = installPackage(sh, distribution(), successfulAutoConfiguration());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        verifySecurityAutoConfigured(installation);
        assertNotNull(installation.getElasticPassword());
        // We remove everything. We don't need to be precise and remove only auto-configuration, the rest are commented out either way
        Path yml = installation.config("elasticsearch.yml");
        Files.write(yml, List.of(), TRUNCATE_EXISTING);

        Shell.Result result = installation.executables().nodeReconfigureTool.run("--enrollment-token a-token", "y", true);
        assertThat(result.exitCode, equalTo(ExitCodes.USAGE)); //
        assertThat(result.stderr, containsString("Expected configuration is missing from elasticsearch.yml."));
    }

    public void test72ReconfigureRetainsUserSettings() throws Exception {
        cleanup();
        assertRemoved(distribution());
        installation = installPackage(sh, distribution(), successfulAutoConfiguration());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        verifySecurityAutoConfigured(installation);
        assertNotNull(installation.getElasticPassword());
        // We remove everything. We don't need to be precise and remove only auto-configuration, the rest are commented out either way
        Path yml = installation.config("elasticsearch.yml");
        List<String> allLines = Files.readAllLines(yml);
        // Replace a comment we know exists in the auto-configuration stanza, with a user defined setting
        allLines.set(
            allLines.indexOf("# All the nodes use the same key and certificate on the inter-node connection"),
            "cluster.name: testclustername"
        );
        allLines.add("node.name: testnodename");
        Files.write(yml, allLines, TRUNCATE_EXISTING);

        // We cannot run two packaged installations simultaneously here so that we can test that the second node enrolls successfully
        // We trigger with an invalid enrollment token, to verify that we removed the existing auto-configuration
        Shell.Result result = installation.executables().nodeReconfigureTool.run("--enrollment-token thisisinvalid", "y", true);
        assertThat(result.exitCode, equalTo(ExitCodes.DATA_ERROR)); // invalid enrollment token
        verifySecurityNotAutoConfigured(installation);
        // Check that user configuration , both inside and outside the autocofiguration stanza, was retained
        Path editedYml = installation.config("elasticsearch.yml");
        List<String> newConfigurationLines = Files.readAllLines(editedYml);
        assertThat(newConfigurationLines, hasItem("cluster.name: testclustername"));
        assertThat(newConfigurationLines, hasItem("node.name: testnodename"));
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
