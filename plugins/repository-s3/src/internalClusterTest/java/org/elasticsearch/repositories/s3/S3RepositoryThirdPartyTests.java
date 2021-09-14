/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.s3;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class S3RepositoryThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(S3RepositoryPlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.s3.account"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.key"), not(blankOrNullString()));
        assertThat(System.getProperty("test.s3.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", System.getProperty("test.s3.account"));
        secureSettings.setString("s3.client.default.secret_key", System.getProperty("test.s3.key"));
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
        Settings.Builder settings = Settings.builder()
            .put("bucket", System.getProperty("test.s3.bucket"))
            .put("base_path", System.getProperty("test.s3.base", "testpath"));
        final String endpoint = System.getProperty("test.s3.endpoint");
        if (endpoint != null) {
            settings.put("endpoint", endpoint);
        } else {
            // only test different storage classes when running against the default endpoint, i.e. a genuine S3 service
            if (randomBoolean()) {
                final String storageClass
                    = randomFrom("standard", "reduced_redundancy", "standard_ia", "onezone_ia", "intelligent_tiering");
                logger.info("--> using storage_class [{}]", storageClass);
                settings.put("storage_class", storageClass);
            }
        }
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
            .setType("s3")
            .setSettings(settings).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }
}
