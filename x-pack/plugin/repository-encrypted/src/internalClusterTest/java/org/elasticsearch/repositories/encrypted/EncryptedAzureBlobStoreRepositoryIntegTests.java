/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.azure.AzureBlobStoreRepositoryTests;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.encrypted.EncryptedRepository.DEK_ROOT_CONTAINER;
import static org.elasticsearch.repositories.encrypted.EncryptedRepository.getEncryptedBlobByteLength;
import static org.hamcrest.Matchers.hasSize;

public final class EncryptedAzureBlobStoreRepositoryIntegTests extends AzureBlobStoreRepositoryTests {
    private static final int NUMBER_OF_TEST_REPOSITORIES = 32;
    private static final List<String> repositoryNames = new ArrayList<>(NUMBER_OF_TEST_REPOSITORIES);

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());

    @BeforeClass
    public static void preGenerateRepositoryNames() {
        assumeFalse("Should only run when encrypted repo is enabled", EncryptedRepositoryPlugin.isDisabled());
        for (int i = 0; i < NUMBER_OF_TEST_REPOSITORIES; i++) {
            repositoryNames.add("test-repo-" + i);
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), License.LicenseType.TRIAL.getTypeName());
        MockSecureSettings superSecureSettings = (MockSecureSettings) settingsBuilder.getSecureSettings();
        superSecureSettings.merge(nodeSecureSettings());
        return settingsBuilder.build();
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
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateEncryptedRepositoryPlugin.class, TestAzureRepositoryPlugin.class);
    }

    @Override
    protected String repositoryType() {
        return EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME;
    }

    @Override
    protected BlobStore newBlobStore(BlobStoreRepository blobStoreRepository) {
        EncryptedRepository.EncryptedBlobStore blobStore = (EncryptedRepository.EncryptedBlobStore) super.newBlobStore(blobStoreRepository);
        if (false == blobStoreRepository.isReadOnly()) {
            PlainActionFuture.get(
                l -> threadPool.generic().execute(ActionRunnable.run(l, () -> blobStore.maybeInitializePasswordGeneration()))
            );
        }
        return blobStore;
    }

    @Override
    protected Settings repositorySettings(String repositoryName) {
        return Settings.builder()
            .put(super.repositorySettings(repositoryName))
            .put(EncryptedRepositoryPlugin.DELEGATE_TYPE_SETTING.getKey(), "azure")
            .put(EncryptedRepositoryPlugin.PASSWORD_NAME_SETTING.getKey(), repositoryName)
            .build();
    }

    @Override
    protected void assertEmptyRepo(Map<String, BytesReference> blobsMap) {
        List<String> blobs = blobsMap.keySet()
            .stream()
            .filter(blob -> false == blob.contains("index"))
            .filter(blob -> false == blob.contains(DEK_ROOT_CONTAINER)) // encryption metadata "leaks"
            .collect(Collectors.toList());
        assertThat("Only index blobs should remain in repository but found " + blobs, blobs, hasSize(0));
    }

    @Override
    protected long blobLengthFromContentLength(long contentLength) {
        return getEncryptedBlobByteLength(contentLength);
    }

}
