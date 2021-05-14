/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;

import java.util.Base64;
import java.util.Collection;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GoogleCloudStorageThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());

        if (Strings.isNullOrEmpty(System.getProperty("test.google.endpoint")) == false) {
            builder.put("gcs.client.default.endpoint", System.getProperty("test.google.endpoint"));
        }

        if (Strings.isNullOrEmpty(System.getProperty("test.google.tokenURI")) == false) {
            builder.put("gcs.client.default.token_uri", System.getProperty("test.google.tokenURI"));
        }

        return builder.build();
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.google.account"), not(blankOrNullString()));
        assertThat(System.getProperty("test.google.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile("gcs.client.default.credentials_file",
            Base64.getDecoder().decode(System.getProperty("test.google.account")));
        return secureSettings;
    }

    @Override
    protected void createRepository(final String repoName) {
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
            .setType("gcs")
            .setSettings(Settings.builder()
                .put("bucket", System.getProperty("test.google.bucket"))
                .put("base_path", System.getProperty("test.google.base", "/"))
            ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }
}
