/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.license.AbstractLicensesIntegrationTestCase;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.xpack.logsdb.SyntheticSourceLicenseServiceTests.createGoldOrPlatinumLicense;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class LegacyLicenceIntegrationTests extends AbstractLicensesIntegrationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(P.class);
    }

    @Before
    public void setup() throws Exception {
        wipeAllLicenses();
        ensureGreen();
        License license = createGoldOrPlatinumLicense();
        putLicense(license);
        ensureGreen();
    }

    public void testSyntheticSourceUsageDisallowed() throws Exception {
        String indexName = "test";
        var settings = Settings.builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic").build();
        createIndex(indexName, settings);
        var response = admin().indices().getSettings(new GetSettingsRequest().indices(indexName)).actionGet();
        assertThat(
            response.getIndexToSettings().get(indexName).get(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()),
            equalTo("STORED")
        );
    }

    public void testSyntheticSourceUsageWithLegacyLicense() throws Exception {
        String indexName = ".profiling-stacktraces";
        var settings = Settings.builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic").build();
        createIndex(indexName, settings);
        var response = admin().indices().getSettings(new GetSettingsRequest().indices(indexName)).actionGet();
        assertThat(
            response.getIndexToSettings().get(indexName).get(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()),
            equalTo("synthetic")
        );
    }

    public static class P extends LocalStateCompositeXPackPlugin {

        public P(final Settings settings, final Path configPath) {
            super(settings, configPath);
            plugins.add(new LogsDBPlugin(settings) {
                @Override
                protected XPackLicenseState getLicenseState() {
                    return P.this.getLicenseState();
                }

                @Override
                protected LicenseService getLicenseService() {
                    return P.this.getLicenseService();
                }
            });
        }

    }
}
