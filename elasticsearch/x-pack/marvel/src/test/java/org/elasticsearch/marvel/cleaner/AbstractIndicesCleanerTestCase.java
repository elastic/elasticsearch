/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.cleaner;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.marvel.MarvelSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.VersionUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Locale;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public abstract class AbstractIndicesCleanerTestCase extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL.getKey(), "-1")
                .put(MarvelSettings.HISTORY_DURATION.getKey(), "-1");
        return settings.build();
    }

    public void testNothingToDelete() throws Exception {
        internalCluster().startNode();

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(0);
    }

    public void testDeleteIndex() throws Exception {
        internalCluster().startNode();

        createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now().minusDays(10));
        assertIndicesCount(1);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(10));
        assertIndicesCount(0);
    }

    public void testIgnoreCurrentDataIndex() throws Exception {
        internalCluster().startNode();

        createDataIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now().minusDays(10));
        assertIndicesCount(1);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(1);
    }

    public void testIgnoreDataIndicesInOtherVersions() throws Exception {
        internalCluster().startNode();

        createIndex(MarvelSettings.LEGACY_DATA_INDEX_NAME, now().minusYears(1));
        createDataIndex(0, now().minusDays(10));
        createDataIndex(Integer.MAX_VALUE, now().minusDays(20));
        assertIndicesCount(3);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(3);
    }

    public void testIgnoreCurrentTimestampedIndex() throws Exception {
        internalCluster().startNode();

        createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now().minusDays(10));
        createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now());
        assertIndicesCount(2);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(1);
    }

    public void testIgnoreTimestampedIndicesInOtherVersions() throws Exception {
        internalCluster().startNode();

        createTimestampedIndex(0, now().minusDays(10));
        createTimestampedIndex(Integer.MAX_VALUE, now().minusDays(10));
        assertIndicesCount(2);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(2);
    }

    public void testDeleteIndices() throws Exception {
        internalCluster().startNode();

        CleanerService.Listener listener = getListener();

        final DateTime now = now();
        createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now.minusYears(1));
        createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now.minusMonths(6));
        createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now.minusMonths(1));
        createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now.minusDays(10));
        createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now.minusDays(1));
        assertIndicesCount(5);

        // Clean indices that have expired two years ago
        listener.onCleanUpIndices(years(2));
        assertIndicesCount(5);

        // Clean indices that have expired 8 months ago
        listener.onCleanUpIndices(months(8));
        assertIndicesCount(4);

        // Clean indices that have expired 3 months ago
        listener.onCleanUpIndices(months(3));
        assertIndicesCount(3);

        // Clean indices that have expired 15 days ago
        listener.onCleanUpIndices(days(15));
        assertIndicesCount(2);

        // Clean indices that have expired 7 days ago
        listener.onCleanUpIndices(days(7));
        assertIndicesCount(1);

        // Clean indices until now
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(0);
    }

    public void testRetentionAsGlobalSetting() throws Exception {
        final int max = 10;
        final int retention = randomIntBetween(1, max);
        internalCluster().startNode(Settings.builder().put(MarvelSettings.HISTORY_DURATION.getKey(),
                String.format(Locale.ROOT, "%dd", retention)));

        final DateTime now = now();
        for (int i = 0; i < max; i++) {
            createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now.minusDays(i));
        }
        assertIndicesCount(max);

        // Clean indices that have expired for N days, as specified in the global retention setting
        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(retention));
        assertIndicesCount(retention);
    }

    public void testRetentionAsExporterSetting() throws Exception {
        final int max = 10;

        // Default retention is between 3 and max days
        final int defaultRetention = randomIntBetween(3, max);
        internalCluster().startNode(Settings.builder().put(MarvelSettings.HISTORY_DURATION.getKey(),
                String.format(Locale.ROOT, "%dd", defaultRetention)));

        final DateTime now = now();
        for (int i = 0; i < max; i++) {
            createTimestampedIndex(MarvelTemplateUtils.TEMPLATE_VERSION, now.minusDays(i));
        }
        assertIndicesCount(max);

        // Exporter retention is between 0 and the default retention
        final int exporterRetention = randomIntBetween(1, defaultRetention);
        assertThat(exporterRetention, lessThanOrEqualTo(defaultRetention));

        // Updates the retention setting for the exporter
        Exporters exporters = internalCluster().getInstance(Exporters.class);
        for (Exporter exporter : exporters) {
            Settings transientSettings = Settings.builder().put("xpack.monitoring.agent.exporters." + exporter.name() + "." +
                    MarvelSettings.HISTORY_DURATION_SETTING_NAME, String.format(Locale.ROOT, "%dd", exporterRetention)).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(transientSettings));
        }

        // Move to GOLD license
        for (MarvelLicensee licensee : internalCluster().getInstances(MarvelLicensee.class)) {
            licensee.onChange(new Licensee.Status(License.OperationMode.GOLD, LicenseState.ENABLED));
        }

        // Try to clean indices using the global setting
        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(defaultRetention));

        // Checks that indices have been deleted according to
        // the retention configured at exporter level
        assertIndicesCount(exporterRetention);
    }

    protected CleanerService.Listener getListener() {
        Exporters exporters = internalCluster().getInstance(Exporters.class);
        for (Exporter exporter : exporters) {
            if (exporter instanceof CleanerService.Listener) {
                return (CleanerService.Listener) exporter;
            }
        }
        throw new IllegalStateException("unable to find listener");
    }

    private MonitoringDoc randomMonitoringDoc() {
        return new MonitoringDoc(randomFrom(MonitoredSystem.values()).getSystem(), VersionUtils.randomVersion(random()).toString());
    }

    /**
     * Creates a monitoring data index in a given version.
     */
    protected void createDataIndex(int version, DateTime creationDate) {
        createIndex(new MockDataIndexNameResolver(version).index(randomMonitoringDoc()), creationDate);
    }

    /**
     * Creates a monitoring timestamped index in a given version.
     */
    protected void createTimestampedIndex(int version, DateTime creationDate) {
        MonitoringDoc monitoringDoc = randomMonitoringDoc();
        monitoringDoc.setTimestamp(creationDate.getMillis());

        MonitoringIndexNameResolver.Timestamped resolver = new MockTimestampedIndexNameResolver(MonitoredSystem.ES, version);
        createIndex(resolver.index(monitoringDoc), creationDate);
    }

    protected abstract void createIndex(String name, DateTime creationDate);

    protected abstract void assertIndicesCount(int count) throws Exception;

    private static TimeValue years(int years) {
        DateTime now = now();
        return TimeValue.timeValueMillis(now.getMillis() - now.minusYears(years).getMillis());
    }

    private static TimeValue months(int months) {
        DateTime now = now();
        return TimeValue.timeValueMillis(now.getMillis() - now.minusMonths(months).getMillis());
    }

    private static TimeValue days(int days) {
        return TimeValue.timeValueHours(days * 24);
    }

    private static DateTime now() {
        return new DateTime(DateTimeZone.UTC);
    }
}
