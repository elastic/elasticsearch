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
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EncryptedBlobStoreRepositoryIntegTests extends ESBlobStoreRepositoryIntegTestCase {

    private static List<String> repositoryNames;

    @BeforeClass
    private static void preGenerateRepositoryNames() {
        repositoryNames = Collections.synchronizedList(randomList(100, 100, () -> "passwordPassword"));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        for (String repositoryName : repositoryNames) {
            secureSettings.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                    getConcreteSettingForNamespace(repositoryName).getKey(), randomAlphaOfLength(10));
        }
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
                .setSecureSettings(secureSettings)
                .build();
    }

    @Override
    protected String randomRepositoryName() {
        return repositoryNames.remove(0);
    }

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
