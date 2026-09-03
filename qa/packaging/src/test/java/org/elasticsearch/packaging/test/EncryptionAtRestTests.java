/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeTrue;

/**
 * Smoke tests that Elasticsearch functions correctly when its data directory lives on an encrypted block device.
 * <p>
 * The suite sets up a dm-crypt/LUKS loopback volume, points {@code path.data} at a directory on that mount, and
 * verifies basic read/write and file-preallocation behaviour. Preallocation is the one area where Elasticsearch
 * interacts directly with the underlying filesystem: {@code fallocate(2)} behaves differently on dm-crypt volumes,
 * so {@link #test30SharedCachePreallocation()} exercises the native-preallocate-or-fallback path by forcing the
 * searchable-snapshots shared blob cache to allocate its cache file on the encrypted mount and confirming the file
 * reaches the expected size.
 * <p>
 * dm-crypt is Linux only, so the whole suite assumes a Linux archive installation.
 */
public class EncryptionAtRestTests extends PackagingTestCase {

    private static final String CRYPT_NAME = "es_encryption_at_rest_test";
    private static final long SHARED_CACHE_SIZE_BYTES = 16 * 1024 * 1024L;

    private static Path imageFile;
    private static Path keyFile;
    private static Path mountPoint;
    private static Path encryptedDataPath;
    private static String loopDevice;

    /**
     * Restricts the suite to Linux archive installations and builds a small dm-crypt/LUKS volume backed by a loopback
     * file, mounted at {@link #mountPoint}.
     */
    @BeforeClass
    public static void setUpEncryptedVolume() throws Exception {
        assumeTrue("encryption at rest via dm-crypt is Linux only", Platforms.LINUX);
        assumeTrue("only archives", distribution().isArchive());
        assumeTrue("cryptsetup must be installed", sh.runIgnoreExitCode("command -v cryptsetup").isSuccess());

        Path workDir = createTempDir("encryption-at-rest");
        imageFile = workDir.resolve("dm-crypt.img");
        keyFile = workDir.resolve("key.secret");
        mountPoint = workDir.resolve("mount");
        encryptedDataPath = mountPoint.resolve("data");

        // sparse 1GiB backing file and a random LUKS key
        sh.run("dd if=/dev/zero of=" + imageFile + " bs=1 count=0 seek=1G");
        sh.run("dd if=/dev/urandom of=" + keyFile + " bs=2k count=1");

        // attach the backing file to a loop device and format it as a LUKS volume
        loopDevice = sh.run("sudo losetup --find --show " + imageFile).stdout().trim();
        sh.run("sudo cryptsetup luksFormat -q --key-file " + keyFile + " " + loopDevice);
        sh.run("sudo cryptsetup open --key-file " + keyFile + " " + loopDevice + " " + CRYPT_NAME);
        sh.run("sudo mkfs.ext4 -q /dev/mapper/" + CRYPT_NAME);

        Files.createDirectories(mountPoint);
        sh.run("sudo mount /dev/mapper/" + CRYPT_NAME + " " + mountPoint);
    }

    @AfterClass
    public static void tearDownEncryptedVolume() {
        if (mountPoint == null) {
            return; // assumptions failed, nothing was set up
        }
        // best effort teardown; use a lazy unmount in case a lingering process still holds the mount
        sh.runIgnoreExitCode("sudo umount " + mountPoint + " || sudo umount -l " + mountPoint);
        if (loopDevice != null) {
            sh.runIgnoreExitCode("sudo cryptsetup close " + CRYPT_NAME);
            sh.runIgnoreExitCode("sudo losetup -d " + loopDevice);
        }
    }

    public void test10Install() throws Exception {
        installation = installArchive(sh, distribution());
        verifyArchiveInstallation(installation, distribution());
        setFileSuperuser("test_superuser", "test_superuser_password");
        // installArchive creates the elasticsearch system user; give it ownership of the encrypted mount
        sh.run("sudo chown -R elasticsearch:elasticsearch " + mountPoint);
        // move the data directory onto the encrypted mount; everything the node persists now lives on the encrypted device
        ServerUtils.addSettingToExistingConfiguration(installation, "path.data", encryptedDataPath.toString());
    }

    /**
     * Exercises the encrypted disk's read/write path before and after a restart. Running {@link #runElasticsearchTests()}
     * on both sides of the stop/start cycle confirms the node can recover its on-disk state from the encrypted mount
     * and continue serving reads and writes.
     */
    public void test20IndexQueryAndSurviveRestart() throws Exception {
        startElasticsearch();
        runElasticsearchTests();
        stopElasticsearch();

        startElasticsearch();
        runElasticsearchTests();
        stopElasticsearch();
    }

    /**
     * Enables the searchable-snapshots shared blob cache, the only production caller of native file preallocation, and
     * verifies its cache file is created at the requested size on the encrypted mount. On dm-crypt {@code fallocate}
     * may not behave normally, so this exercises the best-effort native-preallocate-or-fallback path in
     * {@code SharedBytes#preallocate} against an encrypted filesystem.
     */
    public void test30SharedCachePreallocation() throws Exception {
        withCustomConfig(confPath -> {
            ServerUtils.addSettingToExistingConfiguration(confPath, "xpack.searchable.snapshot.shared_cache.size", "16mb");
            startElasticsearch();
            // the shared cache file is created and preallocated during node startup, directly under path.data
            Path cacheFile = encryptedDataPath.resolve("shared_snapshot_cache");
            Shell.Result stat = sh.run("sudo stat -c %s " + cacheFile);
            assertThat(Long.parseLong(stat.stdout().trim()), equalTo(SHARED_CACHE_SIZE_BYTES));
            stopElasticsearch();
        });
    }
}
