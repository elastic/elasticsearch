/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class AzureRepositoryMissingCredentialsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), AzureRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(AzureStorageSettings.ACCOUNT_SETTING.getConcreteSettingForNamespace("default").getKey(), "test-account");
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).setSecureSettings(secureSettings).build();
    }

    public void testMissingCredentialsException() {
        assertThat(
            asInstanceOf(
                RepositoryVerificationException.class,
                ExceptionsHelper.unwrapCause(
                    safeAwaitFailure(
                        AcknowledgedResponse.class,
                        l -> client().execute(
                            TransportPutRepositoryAction.TYPE,
                            new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo").type("azure"),
                            l
                        )
                    )
                )
            ).getCause().getMessage(),
            allOf(
                containsString("EnvironmentCredential authentication unavailable"),
                containsString("WorkloadIdentityCredential authentication unavailable"),
                containsString("Managed Identity authentication is not available"),
                containsString("SharedTokenCacheCredential authentication unavailable")
            )
        );
    }
}
