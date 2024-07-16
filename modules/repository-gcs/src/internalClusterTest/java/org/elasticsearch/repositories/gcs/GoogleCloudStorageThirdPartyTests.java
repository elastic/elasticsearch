/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.TestUtils;

import com.google.cloud.storage.StorageException;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.rest.RestStatus;
import org.junit.ClassRule;

import java.util.Base64;
import java.util.Collection;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GoogleCloudStorageThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.google.fixture", "true"));

    @ClassRule
    public static GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(USE_FIXTURE, "bucket", "o/oauth2/token");

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());

        if (USE_FIXTURE) {
            builder.put("gcs.client.default.endpoint", fixture.getAddress());
            builder.put("gcs.client.default.token_uri", fixture.getAddress() + "/o/oauth2/token");
        }

        return builder.build();
    }

    @Override
    protected SecureSettings credentials() {
        if (USE_FIXTURE == false) {
            assertThat(System.getProperty("test.google.account"), not(blankOrNullString()));
        }
        assertThat(System.getProperty("test.google.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        if (USE_FIXTURE) {
            secureSettings.setFile("gcs.client.default.credentials_file", TestUtils.createServiceAccount(random()));
        } else {
            secureSettings.setFile(
                "gcs.client.default.credentials_file",
                Base64.getDecoder().decode(System.getProperty("test.google.account"))
            );
        }
        return secureSettings;
    }

    @Override
    protected void createRepository(final String repoName) {
        AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            repoName
        )
            .setType("gcs")
            .setSettings(
                Settings.builder()
                    .put("bucket", System.getProperty("test.google.bucket"))
                    .put("base_path", System.getProperty("test.google.base", "/"))
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    public void testReadFromPositionLargerThanBlobLength() {
        testReadFromPositionLargerThanBlobLength(
            e -> asInstanceOf(StorageException.class, e.getCause()).getCode() == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()
        );
    }
}
