/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.azure.AzureDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.nettycommons.NettyCommonsPlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Regression guard for the {@code auth=ambient} gate on Azure external data sources.
 *
 * <p>End-to-end data read tests are not included because the Azure SDK enforces HTTPS at
 * every blob operation level when using a {@code TokenCredential}, and the IMDS override
 * ({@code AZURE_POD_IDENTITY_AUTHORITY_HOST}) is read by MSAL4J from environment variables
 * only — which Java cannot set at runtime. Both constraints prevent in-process HTTP fixture
 * testing. The S3 IT ({@link FileSourceAmbientAuthIT}) provides the full end-to-end pattern.
 *
 * <p>What IS covered here: the PUT-datasource validator gate rejects {@code auth=ambient}
 * when {@code esql.datasource.ambient_credentials.enabled} is disabled.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FileSourceAzureAmbientAuthIT extends AbstractEsqlIntegTestCase {

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExternalSourceSettings.AMBIENT_CREDENTIALS_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(NettyCommonsPlugin.class);
        plugins.add(AzureDataSourcePlugin.class);
        plugins.add(NdJsonDataSourcePlugin.class);
        plugins.add(TestEncryptionServicePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    /**
     * Verifies that the PUT-datasource validator rejects {@code auth=ambient} when
     * {@code esql.datasource.ambient_credentials.enabled} is disabled.
     */
    public void testAmbientAuthRejectedByValidatorWhenSettingDisabled() {
        var validator = new org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator(
            "azure",
            org.elasticsearch.xpack.esql.datasource.azure.AzureConfiguration::fromMap,
            java.util.Set.of("wasbs", "wasb")
        );
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "ambient"))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.ambient_credentials.enabled"));
    }

}
