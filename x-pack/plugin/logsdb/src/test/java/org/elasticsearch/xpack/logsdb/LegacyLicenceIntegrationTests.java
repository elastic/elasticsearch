/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.license.AbstractLicensesIntegrationTestCase;
import org.elasticsearch.license.GetFeatureUsageRequest;
import org.elasticsearch.license.GetFeatureUsageResponse;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.TransportGetFeatureUsageAction;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.xpack.logsdb.LogsdbLicenseServiceTests.createEnterpriseLicense;
import static org.elasticsearch.xpack.logsdb.LogsdbLicenseServiceTests.createGoldOrPlatinumLicense;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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

    public void testSyntheticSourceUsageDisallowed() {
        createIndexWithSyntheticSourceAndAssertExpectedType("test", "STORED");

        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE_LEGACY, nullValue());
        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE, nullValue());
    }

    public void testSyntheticSourceUsageWithLegacyLicense() {
        createIndexWithSyntheticSourceAndAssertExpectedType(".profiling-stacktraces", "synthetic");

        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE_LEGACY, not(nullValue()));
        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE, nullValue());
    }

    public void testSyntheticSourceUsageWithLegacyLicensePastCutoff() throws Exception {
        // One day after default cutoff date
        long startPastCutoff = LocalDateTime.of(2025, 2, 5, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        putLicense(createGoldOrPlatinumLicense(startPastCutoff));
        ensureGreen();

        createIndexWithSyntheticSourceAndAssertExpectedType(".profiling-stacktraces", "STORED");
        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE_LEGACY, nullValue());
        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE, nullValue());
    }

    public void testSyntheticSourceUsageWithEnterpriseLicensePastCutoff() throws Exception {
        long startPastCutoff = LocalDateTime.of(2025, 11, 12, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        putLicense(createEnterpriseLicense(startPastCutoff));
        ensureGreen();

        createIndexWithSyntheticSourceAndAssertExpectedType(".profiling-traces", "synthetic");
        // also supports non-exceptional indices
        createIndexWithSyntheticSourceAndAssertExpectedType("test", "synthetic");
        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE_LEGACY, nullValue());
        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE, not(nullValue()));
    }

    public void testSyntheticSourceUsageTracksBothLegacyAndRegularFeature() throws Exception {
        createIndexWithSyntheticSourceAndAssertExpectedType(".profiling-traces", "synthetic");

        putLicense(createEnterpriseLicense());
        ensureGreen();

        createIndexWithSyntheticSourceAndAssertExpectedType(".profiling-traces-v2", "synthetic");

        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE_LEGACY, not(nullValue()));
        assertFeatureUsage(LogsdbLicenseService.SYNTHETIC_SOURCE_FEATURE, not(nullValue()));
    }

    private void createIndexWithSyntheticSourceAndAssertExpectedType(String indexName, String expectedType) {
        var settings = Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic").build();
        createIndex(indexName, settings);
        var response = admin().indices().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(indexName)).actionGet();
        assertThat(
            response.getIndexToSettings().get(indexName).get(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()),
            equalTo(expectedType)
        );
    }

    private List<GetFeatureUsageResponse.FeatureUsageInfo> getFeatureUsageInfo() {
        return client().execute(TransportGetFeatureUsageAction.TYPE, new GetFeatureUsageRequest()).actionGet().getFeatures();
    }

    private void assertFeatureUsage(LicensedFeature.Momentary syntheticSourceFeature, Matcher<Object> matcher) {
        GetFeatureUsageResponse.FeatureUsageInfo featureUsage = getFeatureUsageInfo().stream()
            .filter(f -> f.getFamily().equals(LogsdbLicenseService.MAPPINGS_FEATURE_FAMILY))
            .filter(f -> f.getName().equals(syntheticSourceFeature.getName()))
            .findAny()
            .orElse(null);
        assertThat(featureUsage, matcher);
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
