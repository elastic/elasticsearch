/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.elasticsearch.xpack.logsdb.SyntheticSourceIndexSettingsProviderTests.getLogsdbIndexModeSettingsProvider;
import static org.elasticsearch.xpack.logsdb.SyntheticSourceLicenseServiceTests.createGoldOrPlatinumLicense;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SyntheticSourceIndexSettingsProviderLegacyLicenseTests extends ESTestCase {

    private SyntheticSourceIndexSettingsProvider provider;

    @Before
    public void setup() throws Exception {
        long time = LocalDateTime.of(2024, 11, 12, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        License license = createGoldOrPlatinumLicense();
        var licenseState = new XPackLicenseState(() -> time, new XPackLicenseStatus(license.operationMode(), true, null));

        var licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        licenseService.setLicenseState(licenseState);
        var mockLicenseService = mock(LicenseService.class);
        when(mockLicenseService.getLicense()).thenReturn(license);

        SyntheticSourceLicenseService syntheticSourceLicenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        syntheticSourceLicenseService.setLicenseState(licenseState);
        syntheticSourceLicenseService.setLicenseService(mockLicenseService);

        provider = new SyntheticSourceIndexSettingsProvider(
            syntheticSourceLicenseService,
            im -> MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), im.getSettings(), im.getIndex().getName()),
            getLogsdbIndexModeSettingsProvider(false),
            IndexVersion::current
        );
    }

    public void testGetAdditionalIndexSettingsDefault() {
        Settings settings = Settings.builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "SYNTHETIC").build();
        String dataStreamName = "metrics-my-app";
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 0);
        var result = provider.getAdditionalIndexSettings(indexName, dataStreamName, null, null, null, settings, List.of());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()), equalTo("STORED"));
    }

    public void testGetAdditionalIndexSettingsApm() throws IOException {
        Settings settings = Settings.builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "SYNTHETIC").build();
        String dataStreamName = "metrics-apm.app.test";
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 0);
        var result = provider.getAdditionalIndexSettings(indexName, dataStreamName, null, null, null, settings, List.of());
        assertThat(result.size(), equalTo(0));
    }

    public void testGetAdditionalIndexSettingsProfiling() throws IOException {
        Settings settings = Settings.builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "SYNTHETIC").build();
        for (String dataStreamName : new String[] { "profiling-metrics", "profiling-events" }) {
            String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 0);
            var result = provider.getAdditionalIndexSettings(indexName, dataStreamName, null, null, null, settings, List.of());
            assertThat(result.size(), equalTo(0));
        }

        for (String indexName : new String[] { ".profiling-sq-executables", ".profiling-sq-leafframes", ".profiling-stacktraces" }) {
            var result = provider.getAdditionalIndexSettings(indexName, null, null, null, null, settings, List.of());
            assertThat(result.size(), equalTo(0));
        }
    }

    public void testGetAdditionalIndexSettingsTsdb() throws IOException {
        Settings settings = Settings.builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "SYNTHETIC").build();
        String dataStreamName = "metrics-my-app";
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 0);
        var result = provider.getAdditionalIndexSettings(indexName, dataStreamName, IndexMode.TIME_SERIES, null, null, settings, List.of());
        assertThat(result.size(), equalTo(0));
    }

    public void testGetAdditionalIndexSettingsTsdbAfterCutoffDate() throws Exception {
        long start = LocalDateTime.of(2025, 2, 5, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        License license = createGoldOrPlatinumLicense(start);
        long time = LocalDateTime.of(2024, 12, 31, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        var licenseState = new XPackLicenseState(() -> time, new XPackLicenseStatus(license.operationMode(), true, null));

        var licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        licenseService.setLicenseState(licenseState);
        var mockLicenseService = mock(LicenseService.class);
        when(mockLicenseService.getLicense()).thenReturn(license);

        SyntheticSourceLicenseService syntheticSourceLicenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        syntheticSourceLicenseService.setLicenseState(licenseState);
        syntheticSourceLicenseService.setLicenseService(mockLicenseService);

        provider = new SyntheticSourceIndexSettingsProvider(
            syntheticSourceLicenseService,
            im -> MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), im.getSettings(), im.getIndex().getName()),
            getLogsdbIndexModeSettingsProvider(false),
            IndexVersion::current
        );

        Settings settings = Settings.builder().put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "SYNTHETIC").build();
        String dataStreamName = "metrics-my-app";
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 0);
        var result = provider.getAdditionalIndexSettings(indexName, dataStreamName, IndexMode.TIME_SERIES, null, null, settings, List.of());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()), equalTo("STORED"));
    }
}
