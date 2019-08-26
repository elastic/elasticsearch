package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.elasticsearch.packaging.util.Archives.ARCHIVE_OWNER;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.startElasticsearch;
import static org.elasticsearch.packaging.util.Packages.stopElasticsearch;
import static org.elasticsearch.packaging.util.ServerUtils.runElasticsearchTests;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

public class KeystoreManagementTests extends PackagingTestCase {

    /** We need an initially installed package */
    public void test10InstallDistribution() throws Exception {
        if (distribution.isPackage()) {
            assertRemoved(distribution());
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
        verifyCorrectPermissions();

        Platforms.onLinux(() -> {
            Shell.Result r = sh.run(bin.elasticsearchKeystore + " list");
            assertThat(r.stdout, containsString("keystore.seed"));
        });

        Platforms.onWindows(() -> {
            final Shell.Result r = sh.run(bin.elasticsearchKeystore + " list");
            assertThat(r.stdout, containsString("keystore.seed"));
        });
    }

    private void verifyCorrectPermissions() {
        Path keystore = installation.config("elasticsearch.keystore");
        if (distribution.isPackage()) {
            assertThat(keystore, file(File, "root", "elasticsearch", p660));
        } else if (distribution.isArchive()) {
            assertThat(keystore, file(File, ARCHIVE_OWNER, ARCHIVE_OWNER, p660));
        } else {
            throw new AssertionError("Fix this message");
        }
    }

    public void test30AutoCreateKeystore() throws Exception {
        assumeTrue("RPMs and Debs install a keystore file", distribution.isArchive());
        rmKeystoreIfExists();

        Archives.runElasticsearch(installation, sh);
        Archives.stopElasticsearch(installation);

        verifyCorrectPermissions();

        final Installation.Executables bin = installation.executables();
        Platforms.onLinux(() -> {
            Shell.Result result = sh.run(bin.elasticsearchKeystore + " list");
            assertThat(result.stdout, containsString("keystore.seed"));
        });

        Platforms.onWindows(() -> {
            final Shell.Result result = sh.run(bin.elasticsearchKeystore + " list");
            assertThat(result.stdout, containsString("keystore.seed"));
        });
    }

    @Test(timeout = 5 * 60 * 1000)
    public void test40keystorePasswordOnStandardInput() throws Exception {
        assumeTrue("packages will use systemd, which doesnt' handle stdin",
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
        assumeTrue("packages will use systemd, which doesnt' handle stdin",
            distribution.isArchive());
        assumeThat(installation, is(notNullValue()));

        RuntimeException expected = null;
        try {
            Archives.runElasticsearch(installation, sh, "wrong");
        } catch (RuntimeException e) {
            expected = e;
        }
        assertThat(expected, notNullValue());
        assertThat(expected.getMessage(), containsString("Elasticsearch did not start"));
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

            if (distribution.isArchive()) {
                Archives.runElasticsearch(installation, sh);
                runElasticsearchTests();
                Archives.stopElasticsearch(installation);
            } else if (distribution.isPackage()) {
                startElasticsearch(sh);
                runElasticsearchTests();
                stopElasticsearch(sh);
            } else {
                throw new AssertionError("uh oh");
            }
        } finally {
            Files.write(esEnv, originalEnvFile);
            rm(esKeystorePassphraseFile);
        }
    }

    @Test(timeout = 5 * 60 * 1000)
    public void test51wrongKeystorePasswordFromFile() throws Exception {
        String passwordWithNewline = "keystorepass\n";
        Path esKeystorePassphraseFile = installation.config.resolve("eks");
        Path esEnv = installation.bin.resolve("elasticsearch-env");
        byte[] originalEnvFile = Files.readAllBytes(esEnv);

        final Installation.Executables bin = installation.executables();

        RuntimeException expected = null;
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
            sh.run("echo $\'" + passwordWithNewline + passwordWithNewline + "\' | "
                + bin.elasticsearchKeystore + " passwd");

            if (distribution.isArchive()) {
                Archives.runElasticsearch(installation, sh);
            } else if (distribution.isPackage()) {
                startElasticsearch(sh);
            } else {
                throw new AssertionError("uh oh");
            }
        } catch (RuntimeException e) {
            expected = e;
        } finally {
            Files.write(esEnv, originalEnvFile);
            rm(esKeystorePassphraseFile);
        }

        assertNotNull(expected);
        assertThat(expected.getMessage(), anyOf(
            containsString("Elasticsearch did not start"),
            containsString("Command was not successful")));
    }
    private void createKeystore() throws Exception {
        Path keystore = installation.config("elasticsearch.keystore");
        final Installation.Executables bin = installation.executables();
        Platforms.onLinux(() -> {
            if (distribution.isPackage()) {
                sh.run(bin.elasticsearchKeystore + " create");
            } else if (distribution.isArchive()) {
                sh.run("sudo -u " + ARCHIVE_OWNER + " " + bin.elasticsearchKeystore + " create");
            } else {
                throw new AssertionError("Fix this message");
            }
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

}
