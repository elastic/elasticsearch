/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EncryptedFSBlobStoreRepositoryIntegTests extends ESBlobStoreRepositoryIntegTestCase {

    private static List<String> repositoryNames;

    @BeforeClass
    private static void preGenerateRepositoryNames() {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            names.add("test-repo-" + i);
        }
        repositoryNames = Collections.synchronizedList(names);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
                .build();
    }

    @Override
    protected MockSecureSettings nodeSecureSettings(int nodeOrdinal) {
        MockSecureSettings secureSettings = super.nodeSecureSettings(nodeOrdinal);
        for (String repositoryName : repositoryNames) {
            secureSettings.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                    getConcreteSettingForNamespace(repositoryName).getKey(), "passwordPassword");
        }
        return secureSettings;
    }

    @Override
    protected String randomRepositoryName() {
        return repositoryNames.remove(randomIntBetween(0, repositoryNames.size() - 1));
    }

    protected long blobLengthFromDiskLength(BlobMetaData blobMetaData) {
        if (BlobStoreRepository.INDEX_LATEST_BLOB.equals(blobMetaData.name())) {
            // index.latest is not encrypted, hence the size on disk is equal to the content
            return blobMetaData.length();
        } else {
            return DecryptionPacketsInputStream.getDecryptionLength(blobMetaData.length() -
                    EncryptedRepository.MetadataIdentifier.byteLength(), EncryptedRepository.PACKET_LENGTH_IN_BYTES);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateEncryptedRepositoryPlugin.class);
    }

    @Override
    protected String repositoryType() {
        return EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME;
    }

    @Override
    protected Settings repositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put(super.repositorySettings());
        settings.put("location", randomRepoPath());
        settings.put(EncryptedRepositoryPlugin.DELEGATE_TYPE.getKey(), FsRepository.TYPE);
        if (randomBoolean()) {
            long size = 1 << randomInt(10);
            settings.put("chunk_size", new ByteSizeValue(size, ByteSizeUnit.KB));
        }
        return settings.build();
    }

}
