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

package org.elasticsearch.repositories.azure;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AzureStorageCleanupThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(AzureRepositoryPlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.azure.account"), not(blankOrNullString()));
        final boolean hasSasToken = Strings.hasText(System.getProperty("test.azure.sas_token"));
        if (hasSasToken == false) {
            assertThat(System.getProperty("test.azure.key"), not(blankOrNullString()));
        } else {
            assertThat(System.getProperty("test.azure.key"), blankOrNullString());
        }
        assertThat(System.getProperty("test.azure.container"), not(blankOrNullString()));
        assertThat(System.getProperty("test.azure.base"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.default.account", System.getProperty("test.azure.account"));
        if (hasSasToken) {
            secureSettings.setString("azure.client.default.sas_token", System.getProperty("test.azure.sas_token"));
        } else {
            secureSettings.setString("azure.client.default.key", System.getProperty("test.azure.key"));
        }
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster().preparePutRepository(repoName)
            .setType("azure")
            .setSettings(Settings.builder()
                .put("container", System.getProperty("test.azure.container"))
                .put("base_path", System.getProperty("test.azure.base"))
            ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }
}
