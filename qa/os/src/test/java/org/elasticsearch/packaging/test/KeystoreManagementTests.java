/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Packages;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.elasticsearch.packaging.util.Archives.ARCHIVE_OWNER;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

public class KeystoreManagementTests extends PackagingTestCase {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    /** We need an initially installed package */
    public void test10InstallDistribution() throws Exception {
        if (distribution.isPackage()) {
            Packages.assertRemoved(distribution());
        }
        installation = installAndVerify(distribution());
        final Installation.Executables bin = installation.executables();
        Shell.Result r = sh.runIgnoreExitCode(bin.elasticsearchKeystore + " has-passwd");
        assertThat("has-passwd should fail", r.exitCode, not(is(0)));
        assertThat("has-passwd should fail", r.stderr, anyOf(
            containsString("ERROR: Elasticsearch keystore not found"),
            containsString("ERROR: Keystore is not password protected")));
    }

    public void test20CreateKeystoreManually() throws Exception {
        rmKeystoreIfExists();
        createKeystore();

        final Installation.Executables bin = installation.executables();
        verifyKeystorePermissions();

        Shell.Result r = sh.run(bin.elasticsearchKeystore + " list");
        assertThat(r.stdout, containsString("keystore.seed"));
    }

    public void test30AutoCreateKeystore() throws Exception {
        assumeTrue("RPMs and Debs install a keystore file", distribution.isArchive());
        rmKeystoreIfExists();

        Archives.runElasticsearch(installation, sh);
        Archives.stopElasticsearch(installation);

        verifyKeystorePermissions();

        final Installation.Executables bin = installation.executables();
        Shell.Result result = sh.run(bin.elasticsearchKeystore + " list");
        assertThat(result.stdout, containsString("keystore.seed"));
    }

    // TODO[wrb]: remove all timeouts before merging PR
    @Test(timeout = 5 * 60 * 1000)
    public void test40keystorePasswordOnStandardInput() throws Exception {
        assumeTrue("packages will use systemd, which doesn't handle stdin",
            distribution.isArchive());
        assumeThat(installation, is(notNullValue()));

        String password = "keystorepass";

        final Installation.Executables bin = installation.executables();

        // set the password by passing it to stdin twice
        Platforms.onLinux(() ->
            sh.run("echo $\'" + password + "\n" + password + "\n\' | sudo -u " + ARCHIVE_OWNER + " "
                + bin.elasticsearchKeystore + " passwd")
        );
        Platforms.onWindows(() -> {
            sh.run("echo \"" + password + "`r`n" + password + "`r`n\" | " + bin.elasticsearchKeystore + " passwd");
        });

        Archives.runElasticsearch(installation, sh, password);
        ServerUtils.runElasticsearchTests();
        Archives.stopElasticsearch(installation);
    }

    @Test(timeout = 5 * 60 * 1000)
    public void test41wrongKeystorePasswordOnStandardInput() throws Exception {
        assumeTrue("packages will use systemd, which doesn't handle stdin",
            distribution.isArchive());
        assumeThat(installation, is(notNullValue()));

        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Elasticsearch did not start");
        Archives.runElasticsearch(installation, sh, "wrong");
    }

    @Test(timeout = 5 * 60 * 1000)
    public void test50keystorePasswordFromFile() throws Exception {
        String passwordWithNewline = "keystorepass\n";
        Path esKeystorePassphraseFile = installation.config.resolve("eks");
        Path esEnv = installation.bin.resolve("elasticsearch-env");
        byte[] originalEnvFile = Files.readAllBytes(esEnv);

        try {
            Files.createFile(esKeystorePassphraseFile);
            Files.write(esEnv,
                ("ES_KEYSTORE_PASSPHRASE_FILE=" + esKeystorePassphraseFile.toString() + "\n").getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.APPEND);
            Files.write(esKeystorePassphraseFile,
                passwordWithNewline.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.WRITE);

            startElasticsearch();
            ServerUtils.runElasticsearchTests();
            stopElasticsearch();
        } finally {
            Files.write(esEnv, originalEnvFile);
            rm(esKeystorePassphraseFile);
        }
    }

    @Test(timeout = 5 * 60 * 1000)
    public void test51wrongKeystorePasswordFromFile() throws Exception {
        String password = "keystorepass";
        Path esKeystorePassphraseFile = installation.config.resolve("eks");
        Path esEnv = installation.bin.resolve("elasticsearch-env");
        byte[] originalEnvFile = Files.readAllBytes(esEnv);

        final Installation.Executables bin = installation.executables();

        try {
            Files.createFile(esKeystorePassphraseFile);
            Files.write(esEnv,
                ("ES_KEYSTORE_PASSPHRASE_FILE=" + esKeystorePassphraseFile.toString() + "\n").getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.APPEND);
            Files.write(esKeystorePassphraseFile,
                "wrongpassword\n".getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.WRITE);

            sh.run("yes | " + bin.elasticsearchKeystore + " create");
            // set the password by passing it to stdin twice
            Platforms.onLinux(() ->
                sh.run("echo $\'" + password + "\n" + password + "\n\' | sudo -u " + ARCHIVE_OWNER + " "
                    + bin.elasticsearchKeystore + " passwd")
            );
            Platforms.onWindows(() -> {
                sh.run("echo \"" + password + "`r`n" + password + "`r`n\" | " + bin.elasticsearchKeystore + " passwd");
            });

            assertElasticsearchFailsToStart();
        } finally {
            Files.write(esEnv, originalEnvFile);
            rm(esKeystorePassphraseFile);
        }
    }

    private void createKeystore() throws Exception {
        Path keystore = installation.config("elasticsearch.keystore");
        final Installation.Executables bin = installation.executables();
        Platforms.onLinux(() -> {
            selectOnPackaging(
                () -> sh.run(bin.elasticsearchKeystore + " create"),
                () -> sh.run("sudo -u " + ARCHIVE_OWNER + " " + bin.elasticsearchKeystore + " create"));
        });

        // this is a hack around the fact that we can't run a command in the same session as the same user but not as administrator.
        // the keystore ends up being owned by the Administrators group, so we manually set it to be owned by the vagrant user here.
        // from the server's perspective the permissions aren't really different, this is just to reflect what we'd expect in the tests.
        // when we run these commands as a role user we won't have to do this
        Platforms.onWindows(() -> sh.run(
            bin.elasticsearchKeystore + " create; " +
                "$account = New-Object System.Security.Principal.NTAccount 'vagrant'; " +
                "$acl = Get-Acl '" + keystore + "'; " +
                "$acl.SetOwner($account); " +
                "Set-Acl '" + keystore + "' $acl"
        ));
    }

    private void rmKeystoreIfExists() {
        Path keystore = installation.config("elasticsearch.keystore");
        if (Files.exists(keystore)) {
            FileUtils.rm(keystore);
        }
    }

    @FunctionalInterface
    private interface ExceptionalRunnable {
        void run() throws Exception;
    }

    // TODO: This screams "polymorphism," but we don't quite have the right
    // structure in the Distribution class yet...
    private static void selectOnPackaging(ExceptionalRunnable forPackage, ExceptionalRunnable forArchive) throws Exception {
        assertTrue("Distribution must be package or archive",
            distribution.isPackage() || distribution.isArchive());
        if (distribution.isPackage()) {
            forPackage.run();
        } else {
            forArchive.run();
        }
    }

    private void assertElasticsearchFailsToStart() throws Exception {
        exceptionRule.expect(RuntimeException.class);
        selectOnPackaging(
            () -> exceptionRule.expectMessage(containsString("Command was not successful")),
            () -> exceptionRule.expectMessage(containsString("Elasticsearch did not start")));
        startElasticsearch();
    }

    private void verifyKeystorePermissions() throws Exception {
        Path keystore = installation.config("elasticsearch.keystore");
        selectOnPackaging(
            () -> assertThat(keystore, file(File, "root", "elasticsearch", p660)),
            () -> assertThat(keystore, file(File, ARCHIVE_OWNER, ARCHIVE_OWNER, p660)));
    }

    private void stopElasticsearch() throws Exception {
        selectOnPackaging(
            () -> Packages.stopElasticsearch(sh),
            () -> Archives.stopElasticsearch(installation));
    }

    private void startElasticsearch() throws Exception {
        selectOnPackaging(
            () -> Packages.startElasticsearch(sh),
            () -> Archives.runElasticsearch(installation, sh));
    }
}
