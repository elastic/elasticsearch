/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESFsBasedRepositoryIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.repositories.encrypted.EncryptedRepository.getEncryptedBlobByteLength;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public final class EncryptedFSBlobStoreRepositoryIntegTests extends ESFsBasedRepositoryIntegTestCase {
    private static int NUMBER_OF_TEST_REPOSITORIES = 32;

    private static List<String> repositoryNames = new ArrayList<>();

    @BeforeClass
    private static void preGenerateRepositoryNames() {
        for (int i = 0; i < NUMBER_OF_TEST_REPOSITORIES; i++) {
            repositoryNames.add("test-repo-" + i);
        }
    }

    @Override
    protected Settings repositorySettings(String repositoryName) {
        final Settings.Builder settings = Settings.builder()
            .put("compress", randomBoolean())
            .put("location", randomRepoPath())
            .put("delegate_type", FsRepository.TYPE)
            .put("password_name", repositoryName);
        if (randomBoolean()) {
            long size = 1 << randomInt(10);
            settings.put("chunk_size", new ByteSizeValue(size, ByteSizeUnit.KB));
        }
        return settings.build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), License.LicenseType.TRIAL.getTypeName())
            .setSecureSettings(nodeSecureSettings())
            .build();
    }

    protected MockSecureSettings nodeSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        for (String repositoryName : repositoryNames) {
            secureSettings.setString(
                EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
                repositoryName + " ".repeat(14 - repositoryName.length()) // pad to the minimum pass length of 112 bits (14)
            );
        }
        return secureSettings;
    }

    @Override
    protected String randomRepositoryName() {
        return repositoryNames.remove(randomIntBetween(0, repositoryNames.size() - 1));
    }

    @Override
    protected long blobLengthFromContentLength(long contentLength) {
        return getEncryptedBlobByteLength(contentLength);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateEncryptedRepositoryPlugin.class);
    }

    @Override
    protected String repositoryType() {
        return EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME;
    }

    public void testTamperedEncryptionMetadata() throws Exception {
        final String repoName = randomRepositoryName();
        final Path repoPath = randomRepoPath();
        final Settings repoSettings = Settings.builder().put(repositorySettings(repoName)).put("location", repoPath).build();
        createRepository(repoName, repoSettings, true);

        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName).setWaitForCompletion(true).setIndices("other*").get();

        assertAcked(client().admin().cluster().prepareDeleteRepository(repoName));
        createRepository(repoName, Settings.builder().put(repoSettings).put("readonly", randomBoolean()).build(), randomBoolean());

        try (Stream<Path> rootContents = Files.list(repoPath.resolve(EncryptedRepository.DEK_ROOT_CONTAINER))) {
            // tamper all DEKs
            rootContents.filter(Files::isDirectory).forEach(DEKRootPath -> {
                try (Stream<Path> contents = Files.list(DEKRootPath)) {
                    contents.filter(Files::isRegularFile).forEach(DEKPath -> {
                        try {
                            byte[] originalDEKBytes = Files.readAllBytes(DEKPath);
                            // tamper DEK
                            int tamperPos = randomIntBetween(0, originalDEKBytes.length - 1);
                            originalDEKBytes[tamperPos] ^= 0xFF;
                            Files.write(DEKPath, originalDEKBytes);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) internalCluster().getCurrentMasterNodeInstance(
                RepositoriesService.class
            ).repository(repoName);
            RepositoryException e = expectThrows(
                RepositoryException.class,
                () -> PlainActionFuture.<RepositoryData, Exception>get(
                    f -> blobStoreRepository.threadPool().generic().execute(ActionRunnable.wrap(f, blobStoreRepository::getRepositoryData))
                )
            );
            assertThat(e.getMessage(), containsString("the encryption metadata in the repository has been corrupted"));
            e = expectThrows(
                RepositoryException.class,
                () -> client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(true).get()
            );
            assertThat(e.getMessage(), containsString("the encryption metadata in the repository has been corrupted"));
        }
    }

}
