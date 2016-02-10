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
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.IndexNameResolver;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
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
                .put(MarvelSettings.INTERVAL_SETTING.getKey(), "-1")
                .put(CleanerService.HISTORY_SETTING.getKey(), "-1");
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

        createIndex(MarvelSettings.MARVEL_INDICES_PREFIX + "test", now().minusDays(10));
        assertIndicesCount(1);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(10));
        assertIndicesCount(0);
    }

    public void testIgnoreDataIndex() throws Exception {
        internalCluster().startNode();

        createIndex(MarvelSettings.MARVEL_DATA_INDEX_PREFIX + "test", now().minusDays(10));
        assertIndicesCount(1);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(1);
    }

    public void testIgnoreCurrentTimestampedIndex() throws Exception {
        internalCluster().startNode();

        IndexNameResolver indexNameResolver = null;
        for (Exporter exporter : internalCluster().getInstance(Exporters.class)) {
            indexNameResolver = exporter.indexNameResolver();
        }
        assertNotNull(indexNameResolver);

        DateTime tenDaysAgo = now().minusDays(10);
        createIndex(indexNameResolver.resolve(tenDaysAgo.getMillis()), tenDaysAgo);

        DateTime today = now();
        createIndex(indexNameResolver.resolve(today.getMillis()), today);
        assertIndicesCount(2);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
    }

    public void testDeleteIndices() throws Exception {
        internalCluster().startNode();

        CleanerService.Listener listener = getListener();

        final DateTime now = now();
        createIndex(MarvelSettings.MARVEL_INDICES_PREFIX + "one-year-ago", now.minusYears(1));
        createIndex(MarvelSettings.MARVEL_INDICES_PREFIX + "six-months-ago", now.minusMonths(6));
        createIndex(MarvelSettings.MARVEL_INDICES_PREFIX + "one-month-ago", now.minusMonths(1));
        createIndex(MarvelSettings.MARVEL_INDICES_PREFIX + "ten-days-ago", now.minusDays(10));
        createIndex(MarvelSettings.MARVEL_INDICES_PREFIX + "one-day-ago", now.minusDays(1));
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
        internalCluster().startNode(Settings.builder().put(CleanerService.HISTORY_SETTING.getKey(),
                String.format(Locale.ROOT, "%dd", retention)));

        final DateTime now = now();
        for (int i = 0; i < max; i++) {
            createIndex(MarvelSettings.MARVEL_INDICES_PREFIX + String.valueOf(i), now.minusDays(i));
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
        internalCluster().startNode(Settings.builder().put(CleanerService.HISTORY_SETTING.getKey(),
                String.format(Locale.ROOT, "%dd", defaultRetention)));

        final DateTime now = now();
        for (int i = 0; i < max; i++) {
            createIndex(MarvelSettings.MARVEL_INDICES_PREFIX + String.valueOf(i), now.minusDays(i));
        }
        assertIndicesCount(max);

        // Exporter retention is between 0 and the default retention
        final int exporterRetention = randomIntBetween(1, defaultRetention);
        assertThat(exporterRetention, lessThanOrEqualTo(defaultRetention));

        // Updates the retention setting for the exporter
        Exporters exporters = internalCluster().getInstance(Exporters.class);
        for (Exporter exporter : exporters) {
            Settings transientSettings = Settings.builder().put("marvel.agent.exporters." + exporter.name() + "." +
                    CleanerService.HISTORY_DURATION, String.format(Locale.ROOT, "%dd", exporterRetention)).build();
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
