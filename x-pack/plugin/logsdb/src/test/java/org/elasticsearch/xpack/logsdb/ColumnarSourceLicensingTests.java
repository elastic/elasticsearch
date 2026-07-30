/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.xpack.logsdb.LogsdbLicenseServiceTests.createEnterpriseLicense;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the license-based source-mode fallback for the {@code columnar} and {@code logsdb_columnar}
 * index modes. Both modes default to synthetic source but do not support stored source, so they must fall
 * back to {@link SourceFieldMapper.Mode#COLUMNAR_STORED} (not {@link SourceFieldMapper.Mode#STORED}) when
 * an enterprise license is absent.
 *
 * The enforcement lives in {@link LogsdbIndexModeSettingsProvider} — the only {@code IndexSettingProvider}
 * that handles synthetic-source fallback — which is why these tests reside in the logsdb module despite
 * plain {@code columnar} not being a logsdb concept.
 */
public class ColumnarSourceLicensingTests extends ESTestCase {

    private static final String DATA_STREAM_NAME = "logs-app1";

    private static MockLicenseState sharedLicenseState;
    private static LicenseService sharedLicenseService;
    private static XPackLicenseState sharedBasicLicenseState;
    private static LicenseService sharedBasicLicenseService;

    private LogsdbLicenseService enterpriseLicenseService;
    private LogsdbLicenseService basicLicenseService;

    @BeforeClass
    public static void setupClass() throws Exception {
        sharedLicenseState = MockLicenseState.createMock();
        when(sharedLicenseState.isAllowed(any())).thenReturn(true);
        sharedLicenseService = mock(LicenseService.class);
        when(sharedLicenseService.getLicense()).thenReturn(createEnterpriseLicense());

        sharedBasicLicenseState = new XPackLicenseState(() -> 0L, new XPackLicenseStatus(License.OperationMode.BASIC, true, null));
        sharedBasicLicenseService = mock(LicenseService.class);
        when(sharedBasicLicenseService.getLicense()).thenReturn(null);
    }

    @Before
    public void setup() {
        enterpriseLicenseService = new LogsdbLicenseService(Settings.EMPTY);
        enterpriseLicenseService.setLicenseState(sharedLicenseState);
        enterpriseLicenseService.setLicenseService(sharedLicenseService);

        basicLicenseService = new LogsdbLicenseService(Settings.EMPTY);
        basicLicenseService.setLicenseState(sharedBasicLicenseState);
        basicLicenseService.setLicenseService(sharedBasicLicenseService);
    }

    /**
     * Runs the {@link LogsdbIndexModeSettingsProvider} for the given columnar index mode and license service,
     * and returns the additional settings it injects.
     */
    private Settings provideColumnarSettings(IndexMode columnarMode, LogsdbLicenseService licenseService) throws IOException {
        var clusterSettings = Settings.builder().put("cluster.logsdb.enabled", true).build();
        var provider = new LogsdbIndexModeSettingsProvider(licenseService, clusterSettings);
        var logsdbPlugin = new LogsDBPlugin(Settings.EMPTY);
        provider.init(
            im -> MapperTestUtils.newMapperService(
                xContentRegistry(),
                createTempDir(),
                im.getSettings(),
                new IndicesModule(List.of(logsdbPlugin)),
                im.getIndex().getName(),
                logsdbPlugin.getSettings().stream().filter(Setting::hasIndexScope).toArray(Setting<?>[]::new)
            ),
            IndexVersion::current,
            () -> Version.CURRENT,
            true,
            true
        );
        Settings indexSettings = Settings.builder().put(IndexSettings.MODE.getKey(), columnarMode).build();
        Settings.Builder settingsBuilder = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName(DATA_STREAM_NAME, 0),
            DATA_STREAM_NAME,
            columnarMode,
            emptyProject(),
            Instant.now(),
            indexSettings,
            List.of(),
            IndexVersion.current(),
            settingsBuilder
        );
        return builder().put(settingsBuilder.build()).build();
    }

    public void testLogsdbColumnarFallsBackToColumnarStoredWithoutEnterpriseLicense() throws IOException {
        Settings result = provideColumnarSettings(IndexMode.LOGSDB_COLUMNAR, basicLicenseService);
        // Use raw string comparison: INDEX_MAPPER_SOURCE_MODE_SETTING.get() cross-validates against index.mode,
        // which is not present in the additional settings returned by the provider.
        assertEquals(SourceFieldMapper.Mode.COLUMNAR_STORED.name(), result.get(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()));
    }

    public void testColumnarFallsBackToColumnarStoredWithoutEnterpriseLicense() throws IOException {
        Settings result = provideColumnarSettings(IndexMode.COLUMNAR, basicLicenseService);
        assertEquals(SourceFieldMapper.Mode.COLUMNAR_STORED.name(), result.get(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()));
    }

    public void testLogsdbColumnarDoesNotFallBackWithEnterpriseLicense() throws IOException {
        Settings result = provideColumnarSettings(IndexMode.LOGSDB_COLUMNAR, enterpriseLicenseService);
        assertFalse(
            "enterprise license should allow synthetic source; source mode setting must not be injected",
            IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.exists(result)
        );
    }

    public void testColumnarDoesNotFallBackWithEnterpriseLicense() throws IOException {
        Settings result = provideColumnarSettings(IndexMode.COLUMNAR, enterpriseLicenseService);
        assertFalse(
            "enterprise license should allow synthetic source; source mode setting must not be injected",
            IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.exists(result)
        );
    }

    public void testLogsdbColumnarFallsBackToColumnarStoredWithOperatorFallbackFlag() throws IOException {
        enterpriseLicenseService.setSyntheticSourceFallback(true);
        try {
            Settings result = provideColumnarSettings(IndexMode.LOGSDB_COLUMNAR, enterpriseLicenseService);
            assertEquals(
                SourceFieldMapper.Mode.COLUMNAR_STORED.name(),
                result.get(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey())
            );
        } finally {
            enterpriseLicenseService.setSyntheticSourceFallback(false);
        }
    }

    public void testColumnarFallsBackToColumnarStoredWithOperatorFallbackFlag() throws IOException {
        enterpriseLicenseService.setSyntheticSourceFallback(true);
        try {
            Settings result = provideColumnarSettings(IndexMode.COLUMNAR, enterpriseLicenseService);
            assertEquals(
                SourceFieldMapper.Mode.COLUMNAR_STORED.name(),
                result.get(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey())
            );
        } finally {
            enterpriseLicenseService.setSyntheticSourceFallback(false);
        }
    }
}
