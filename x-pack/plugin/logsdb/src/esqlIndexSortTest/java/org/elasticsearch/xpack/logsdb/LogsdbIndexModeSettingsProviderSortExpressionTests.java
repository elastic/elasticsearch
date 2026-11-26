/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexSortParser;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.index.EsqlIndexSortParser;
import org.junit.BeforeClass;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.xpack.logsdb.LogsdbLicenseServiceTests.createEnterpriseLicense;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The logsdb settings for an {@code index.sort} expression, which the ES|QL parser reads. */
public class LogsdbIndexModeSettingsProviderSortExpressionTests extends ESTestCase {

    private static LogsdbLicenseService licenseService;
    private final AtomicInteger newMapperServiceCounter = new AtomicInteger();

    @BeforeClass
    public static void setupClass() throws Exception {
        IndexSortParser.init(new EsqlIndexSortParser());
        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(any())).thenReturn(true);
        LicenseService service = mock(LicenseService.class);
        when(service.getLicense()).thenReturn(createEnterpriseLicense());
        licenseService = new LogsdbLicenseService(Settings.EMPTY);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(service);
    }

    public void testRoutingPathOnSortExpression() throws Exception {
        var settings = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_SETTING.getKey(), "host, message DESC, @timestamp DESC")
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .build();
        Settings result = generateLogsdbSettings(settings);
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), contains("host", "message"));
    }

    public void testRoutingPathOnSortExpressionWithRepeatedField() throws Exception {
        var settings = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_SETTING.getKey(), "host, MV_MIN(message), MV_MAX(message) DESC, @timestamp DESC")
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .build();
        Settings result = generateLogsdbSettings(settings);
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), contains("host", "message"));
    }

    public void testRoutingPathOnSortExpressionMismatch() {
        var settings = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_SETTING.getKey(), "host, message DESC, @timestamp DESC")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host,message,foo")
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .build();
        Exception e = expectThrows(IllegalStateException.class, () -> generateLogsdbSettings(settings));
        assertThat(e.getMessage(), containsString("[index.routing_path:[host, message, foo]], [index.sort:[host, message]]"));
    }

    public void testRoutingPathOnSortExpressionWithComputedKey() throws Exception {
        var settings = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_SETTING.getKey(), "host, DATE_TRUNC(1 hour, @timestamp) DESC, message DESC")
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .build();
        Settings result = generateLogsdbSettings(settings);
        assertThat(IndexMetadata.INDEX_ROUTING_PATH.get(result), contains("host", "message"));
    }

    public void testSortAndHostNameWithSortExpression() throws Exception {
        var settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            .put(IndexSortConfig.INDEX_SORT_SETTING.getKey(), "foo, bar DESC")
            .build();
        Settings result = generateLogsdbSettings(settings);
        assertFalse(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.get(result));
        assertFalse(IndexSettings.LOGSDB_ADD_HOST_NAME_FIELD.get(result));
        assertEquals(0, newMapperServiceCounter.get());
    }

    private Settings generateLogsdbSettings(Settings settings) throws IOException {
        var provider = new LogsdbIndexModeSettingsProvider(licenseService, Settings.builder().put("cluster.logsdb.enabled", true).build());
        var logsdbPlugin = new LogsDBPlugin(settings);
        provider.init(im -> {
            newMapperServiceCounter.incrementAndGet();
            return MapperTestUtils.newMapperService(
                xContentRegistry(),
                createTempDir(),
                im.getSettings(),
                new IndicesModule(List.of(logsdbPlugin)),
                im.getIndex().getName(),
                logsdbPlugin.getSettings().stream().filter(Setting::hasIndexScope).toArray(Setting<?>[]::new)
            );
        }, IndexVersion::current, () -> Version.CURRENT, true, true);
        Settings.Builder settingsBuilder = builder();
        provider.provideAdditionalSettings(
            DataStream.getDefaultBackingIndexName("logs-app1", 0),
            "logs-app1",
            IndexMode.LOGSDB,
            false,
            emptyProject(),
            Instant.now(),
            settings,
            List.of(),
            IndexVersion.current(),
            settingsBuilder
        );
        return builder().put(settingsBuilder.build()).build();
    }
}
