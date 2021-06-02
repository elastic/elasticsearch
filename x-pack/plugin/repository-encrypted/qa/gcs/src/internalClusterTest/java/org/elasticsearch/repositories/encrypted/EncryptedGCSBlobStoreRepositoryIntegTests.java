/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.gcs.GoogleCloudStorageBlobStoreRepositoryTests;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.encrypted.EncryptedRepository.DEK_ROOT_CONTAINER;
import static org.elasticsearch.repositories.encrypted.EncryptedRepository.getEncryptedBlobByteLength;
import static org.hamcrest.Matchers.hasSize;

public final class EncryptedGCSBlobStoreRepositoryIntegTests extends GoogleCloudStorageBlobStoreRepositoryTests {

    private static List<String> repositoryNames;

    @BeforeClass
    public static void preGenerateRepositoryNames() {
        assumeFalse("Should only run when encrypted repo is enabled", EncryptedRepositoryPlugin.isDisabled());
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            names.add("test-repo-" + i);
        }
        repositoryNames = Collections.synchronizedList(names);
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
        return Arrays.asList(LocalStateEncryptedRepositoryPlugin.class, TestGoogleCloudStoragePlugin.class);
    }

    @Override
    protected String repositoryType() {
        return EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME;
    }

    @Override
    protected Settings repositorySettings(String repositoryName) {
        return Settings.builder()
            .put(super.repositorySettings(repositoryName))
            .put(EncryptedRepositoryPlugin.DELEGATE_TYPE_SETTING.getKey(), "gcs")
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
