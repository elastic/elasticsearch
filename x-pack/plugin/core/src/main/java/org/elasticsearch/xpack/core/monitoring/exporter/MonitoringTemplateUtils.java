/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.monitoring.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.regex.Pattern;

public final class MonitoringTemplateUtils {

    private static final String TEMPLATE_FILE = "/monitoring-%s.json";
    private static final String TEMPLATE_VERSION_PROPERTY = Pattern.quote("${monitoring.template.version}");

    /**
     * The last version of X-Pack that updated the templates and pipelines.
     * <p>
     * It may be possible for this to diverge between templates and pipelines, but for now they're the same.
     */
    public static final int LAST_UPDATED_VERSION = Version.V_7_0_0.id;

    /**
     * Current version of templates used in their name to differentiate from breaking changes (separate from product version).
     */
    public static final String TEMPLATE_VERSION = "7";

    /**
     * The previous version of templates, which we still support via the REST /_monitoring/bulk endpoint
     */
    public static final String OLD_TEMPLATE_VERSION = "6";

    /**
     * IDs of templates that can be used with {@linkplain #loadTemplate(String) loadTemplate}.
     */
    public static final String[] TEMPLATE_IDS = { "alerts", "es", "kibana", "logstash", "beats" };

    private MonitoringTemplateUtils() { }

    /**
     * Get a template name for any template ID.
     *
     * @param id The template identifier.
     * @return Never {@code null} {@link String} prefixed by ".monitoring-". and post-fixed by the version
     * @see #TEMPLATE_IDS
     */
    public static String templateName(final String id) {
        return ".monitoring-" + id + "-" + TEMPLATE_VERSION;
    }

    public static String loadTemplate(final String id) {
        String resource = String.format(Locale.ROOT, TEMPLATE_FILE, id);
        return TemplateUtils.loadTemplate(resource, TEMPLATE_VERSION, TEMPLATE_VERSION_PROPERTY);
    }


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
