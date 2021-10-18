/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.monitoring.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;

import java.time.Instant;

public final class MonitoringTemplateUtils {

    /**
     * The last version of X-Pack that updated the templates and pipelines.
     * <p>
     * It may be possible for this to diverge between templates and pipelines, but for now they're the same.
     *
     * Note that the templates were last updated in 7.11.0, but the versions were not updated, meaning that upgrades
     * to 7.11.0 would not have updated the templates. See https://github.com/elastic/elasticsearch/pull/69317.
     */
    public static final int LAST_UPDATED_VERSION = Version.V_7_14_0.id;

    /**
     * Current version of templates used in their name to differentiate from breaking changes (separate from product version).
     * Version 7 has the same structure as version 6, but uses the `_doc` type.
     */
    public static final String TEMPLATE_VERSION = "7";
    /**
     * The previous version of templates, which we still support via the REST /_monitoring/bulk endpoint because
     * nothing changed for those documents.
     */
    public static final String OLD_TEMPLATE_VERSION = "6";

    private MonitoringTemplateUtils() { }

    /**
     * Get the index name given a specific date format, a monitored system and a timestamp.
     *
     * @param formatter the {@link DateFormatter} to use to compute the timestamped index name
     * @param system the {@link MonitoredSystem} for which the index name is computed
     * @param timestamp the timestamp value to use to compute the timestamped index name
     * @return the index name as a @{link String}
     */
    public static String indexName(final DateFormatter formatter, final MonitoredSystem system, final long timestamp) {
        return ".monitoring-" + system.getSystem() + "-" + TEMPLATE_VERSION + "-" + formatter.format(Instant.ofEpochMilli(timestamp));
    }
}
