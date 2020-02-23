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
package org.elasticsearch.repositories.s3;

import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.encrypted.DecryptionPacketsInputStream;
import org.elasticsearch.repositories.encrypted.EncryptedRepository;
import org.elasticsearch.repositories.encrypted.EncryptedRepositoryPlugin;
import org.elasticsearch.repositories.encrypted.LocalStateEncryptedRepositoryPlugin;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EncryptedS3BlobStoreRepositoryIntegTests extends S3BlobStoreRepositoryTests {

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
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), License.LicenseType.TRIAL.getTypeName())
                .build();
    }

    @Override
    protected MockSecureSettings nodeSecureSettings(int nodeOrdinal) {
        MockSecureSettings secureSettings = super.nodeSecureSettings(nodeOrdinal);
        for (String repositoryName : repositoryNames) {
            secureSettings.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                    getConcreteSettingForNamespace(repositoryName).getKey(), "password" + repositoryName);
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
        return Arrays.asList(LocalStateEncryptedRepositoryPlugin.class, TestS3RepositoryPlugin.class);
    }

    @Override
    protected String repositoryType() {
        return EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME;
    }

    @Override
    protected Settings repositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put(super.repositorySettings());
        settings.put(EncryptedRepositoryPlugin.DELEGATE_TYPE.getKey(), S3Repository.TYPE);
        return settings.build();
    }

    @Override
    public void testEnforcedCooldownPeriod() {
        // this test is not applicable for the encrypted repository because it verifies behavior which pertains to snapshots that must
        // be created before the encrypted repository was introduced, hence no such encrypted snapshots can possibly exist
    }

}
