/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.cleaner;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.monitoring.MonitoringField;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0)
public abstract class AbstractIndicesCleanerTestCase extends MonitoringIntegTestCase {

    static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);
    static Integer INDEX_TEMPLATE_VERSION = null;

    public void testNothingToDelete() throws Exception {
        internalCluster().startNode();

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(0);
    }

    public void testDeleteIndex() throws Exception {
        internalCluster().startNode();

        createTimestampedIndex(now().minusDays(10));
        assertIndicesCount(1);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(10));
        assertIndicesCount(0);
    }

    public void testIgnoreCurrentAlertsIndex() throws Exception {
        internalCluster().startNode();

        // Will be deleted
        createTimestampedIndex(now().minusDays(10));

        // Won't be deleted
        createAlertsIndex(now().minusYears(1));

        assertIndicesCount(2);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(1);
    }

    public void testDoesNotIgnoreIndicesInOtherVersions() throws Exception {
        internalCluster().startNode();

        // Will be deleted
        createTimestampedIndex(now().minusDays(10));
        createIndex(".monitoring-data-2", now().minusDays(10));
        createAlertsIndex(now().minusYears(1), MonitoringTemplateUtils.OLD_TEMPLATE_VERSION);
        createTimestampedIndex(now().minusDays(10), "0");
        createTimestampedIndex(now().minusDays(10), "1");
        createTimestampedIndex(now().minusYears(1), MonitoringTemplateUtils.OLD_TEMPLATE_VERSION);
        // In the past, this index would not be deleted, but starting in 6.x the monitoring cluster
        // will be required to be a newer template version than the production cluster, so the index
        // pushed to it will never be "unknown" in terms of their version
        createTimestampedIndex(now().minusDays(10), String.valueOf(Integer.MAX_VALUE));

        // Won't be deleted
        createAlertsIndex(now().minusYears(1));

        assertIndicesCount(8);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(1);
    }

    public void testIgnoreCurrentTimestampedIndex() throws Exception {
        internalCluster().startNode();

        // Will be deleted
        createTimestampedIndex(now().minusDays(10));

        // Won't be deleted
        createTimestampedIndex(now());

        assertIndicesCount(2);

        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(0));
        assertIndicesCount(1);
    }

    public void testDeleteIndices() throws Exception {
        internalCluster().startNode();

        CleanerService.Listener listener = getListener();

        final ZonedDateTime now = now();
        createTimestampedIndex(now.minusYears(1));
        createTimestampedIndex(now.minusMonths(6));
        createTimestampedIndex(now.minusMonths(1));
        createTimestampedIndex(now.minusDays(10));
        createTimestampedIndex(now.minusDays(1));
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
        internalCluster().startNode(Settings.builder().put(MonitoringField.HISTORY_DURATION.getKey(),
                String.format(Locale.ROOT, "%dd", retention)));

        final ZonedDateTime now = now();
        for (int i = 0; i < max; i++) {
            createTimestampedIndex(now.minusDays(i));
        }
        assertIndicesCount(max);

        // Clean indices that have expired for N days, as specified in the global retention setting
        CleanerService.Listener listener = getListener();
        listener.onCleanUpIndices(days(retention));
        assertIndicesCount(retention);
    }

    protected CleanerService.Listener getListener() {
        Exporters exporters = internalCluster().getInstance(Exporters.class, internalCluster().getMasterName());
        for (Exporter exporter : exporters.getEnabledExporters()) {
            if (exporter instanceof CleanerService.Listener) {
                return (CleanerService.Listener) exporter;
            }
        }
        throw new IllegalStateException("unable to find listener");
    }

    /**
     * Creates a monitoring alerts index from the current version.
     */
    protected void createAlertsIndex(final ZonedDateTime creationDate) {
        createAlertsIndex(creationDate, MonitoringTemplateUtils.TEMPLATE_VERSION);
    }

    /**
     * Creates a monitoring alerts index from the specified version.
     */
    protected void createAlertsIndex(final ZonedDateTime creationDate, final String version) {
        createIndex(".monitoring-alerts-" + version, creationDate);
    }

    /**
     * Creates a watcher history index from the current version.
     */
    protected void createWatcherHistoryIndex(final ZonedDateTime creationDate) {
        if (INDEX_TEMPLATE_VERSION == null) {
            INDEX_TEMPLATE_VERSION = randomIntBetween(1, 20);
        }
        createWatcherHistoryIndex(creationDate, String.valueOf(INDEX_TEMPLATE_VERSION));
    }

    /**
     * Creates a watcher history index from the specified version.
     */
    protected void createWatcherHistoryIndex(final ZonedDateTime creationDate, final String version) {
        final String index = ".watcher-history-" + version + "-" + DATE_FORMATTER.format(creationDate);

        createIndex(index, creationDate);
    }

    /**
     * Creates a monitoring timestamped index using the current template version.
     */
    protected void createTimestampedIndex(ZonedDateTime creationDate) {
        createTimestampedIndex(creationDate, MonitoringTemplateUtils.TEMPLATE_VERSION);
    }

    /**
     * Creates a monitoring timestamped index using a given template version.
     */
    protected void createTimestampedIndex(ZonedDateTime creationDate, String version) {
        final String index = ".monitoring-es-" + version + "-" + DATE_FORMATTER.format(creationDate);
        createIndex(index, creationDate);
    }

    protected abstract void createIndex(String name, ZonedDateTime creationDate);

    protected abstract void assertIndicesCount(int count) throws Exception;

    protected static TimeValue years(int years) {
        ZonedDateTime now = now();
        return TimeValue.timeValueMillis(now.toInstant().toEpochMilli() - now.minusYears(years).toInstant().toEpochMilli());
    }

    protected static TimeValue months(int months) {
        ZonedDateTime now = now();
        return TimeValue.timeValueMillis(now.toInstant().toEpochMilli()  - now.minusMonths(months).toInstant().toEpochMilli());
    }

    protected static TimeValue days(int days) {
        return TimeValue.timeValueHours(days * 24);
    }

    protected static ZonedDateTime now() {
        return ZonedDateTime.now(ZoneOffset.UTC);
    }
}
