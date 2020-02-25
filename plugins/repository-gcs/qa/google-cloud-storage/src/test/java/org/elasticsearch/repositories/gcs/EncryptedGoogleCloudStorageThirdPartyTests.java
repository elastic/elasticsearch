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
package org.elasticsearch.repositories.gcs;

import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.encrypted.EncryptedRepository;
import org.elasticsearch.repositories.encrypted.EncryptedRepositoryPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class EncryptedGoogleCloudStorageThirdPartyTests extends GoogleCloudStorageThirdPartyTests {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(XPackPlugin.class, EncryptedRepositoryPlugin.class, GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
                .put(super.nodeSettings())
                .put("xpack.license.self_generated.type", "trial")
                .build();
    }

    @Override
    protected SecureSettings credentials() {
        MockSecureSettings secureSettings = (MockSecureSettings) super.credentials();
        secureSettings.setString(EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.
                getConcreteSettingForNamespace("test-encrypted-repo").getKey(), "password-test-repo");
        return secureSettings;
    }

    @Override
    protected void createRepository(final String repoName) {
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster()
                .preparePutRepository("test-encrypted-repo")
                .setType("encrypted")
                .setSettings(Settings.builder()
                        .put("delegate_type", "gcs")
                        .put("bucket", System.getProperty("test.google.bucket"))
                        .put("base_path", System.getProperty("test.google.base", "")
                                + "/" + EncryptedGoogleCloudStorageThirdPartyTests.class.getName() )
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    @Override
    protected void assertCleanupResponse(CleanupRepositoryResponse response, long bytes, long blobs) {
        // TODO cleanup of root blobs does not count the encryption metadata blobs, but the cleanup of blob containers ("indices" folder)
        //  does count them; ideally there should be consistency, one way or the other
        assertThat(response.result().blobs(), equalTo(1L + 2L + 1L /* one metadata blob */));
        // the cleanup stats of the encrypted repository currently includes only some of the metadata blobs (as per above), which are
        // themselves cumbersome to size; but the bytes count is stable
        assertThat(response.result().bytes(), equalTo(244L));
    }

    @Override
    protected void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetaData> blobs) throws Exception {
        // blobs are larger after encryption
        Map<String, BlobMetaData> blobsWithSizeAfterEncryption = new HashMap<>();
        blobs.forEach((name, meta) -> {
            blobsWithSizeAfterEncryption.put(name, new BlobMetaData() {
                @Override
                public String name() {
                    return meta.name();
                }

                @Override
                public long length() {
                    return EncryptedRepository.getEncryptedBlobByteLength(meta.length());
                }
            });
        });
        super.assertBlobsByPrefix(path, prefix, blobsWithSizeAfterEncryption);
    }

    @Override
    protected String getTestRepoName() {
        return "test-encrypted-repo";
    }

}
