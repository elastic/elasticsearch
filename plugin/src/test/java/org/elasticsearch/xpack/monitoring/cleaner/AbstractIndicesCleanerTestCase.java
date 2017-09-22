/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.cleaner;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public abstract class AbstractIndicesCleanerTestCase extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1");
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
        createIndex(".marvel-es-data", now().minusYears(1));
        createIndex(".marvel-es-data-2", now().minusYears(1));
        createIndex(".monitoring-data-2", now().minusDays(10));
        createAlertsIndex(now().minusYears(1), MonitoringTemplateUtils.OLD_TEMPLATE_VERSION);
        createIndex(".marvel-es-2016.05.03");
        createIndex(".marvel-es-2-2016.05.03");
        createTimestampedIndex(now().minusDays(10), "0");
        createTimestampedIndex(now().minusDays(10), "1");
        createTimestampedIndex(now().minusYears(1), MonitoringTemplateUtils.OLD_TEMPLATE_VERSION);
        // In the past, this index would not be deleted, but starting in 6.x the monitoring cluster
        // will be required to be a newer template version than the production cluster, so the index
        // pushed to it will never be "unknown" in terms of their version (relates to the
        // _xpack/monitoring/_setup API)
        createTimestampedIndex(now().minusDays(10), String.valueOf(Integer.MAX_VALUE));

        // Won't be deleted
        createAlertsIndex(now().minusYears(1));

        assertIndicesCount(12);

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

        final DateTime now = now();
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
        internalCluster().startNode(Settings.builder().put(MonitoringSettings.HISTORY_DURATION.getKey(),
                String.format(Locale.ROOT, "%dd", retention)));

        final DateTime now = now();
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
        for (Exporter exporter : exporters) {
            if (exporter instanceof CleanerService.Listener) {
                return (CleanerService.Listener) exporter;
            }
        }
        throw new IllegalStateException("unable to find listener");
    }

    /**
     * Creates a monitoring alerts index from the current version.
     */
    protected void createAlertsIndex(final DateTime creationDate) {
        createAlertsIndex(creationDate, MonitoringTemplateUtils.TEMPLATE_VERSION);
    }

    /**
     * Creates a monitoring alerts index from the current version.
     */
    protected void createAlertsIndex(final DateTime creationDate, final String version) {
        createIndex(".monitoring-alerts-" + version, creationDate);
    }

    /**
     * Creates a monitoring timestamped index using the current template version.
     */
    protected void createTimestampedIndex(DateTime creationDate) {
        createTimestampedIndex(creationDate, MonitoringTemplateUtils.TEMPLATE_VERSION);
    }

    /**
     * Creates a monitoring timestamped index using a given template version.
     */
    protected void createTimestampedIndex(DateTime creationDate, String version) {
        final DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY.MM.dd").withZoneUTC();
        final String index = ".monitoring-es-" + version + "-" + formatter.print(creationDate.getMillis());
        createIndex(index, creationDate);
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
