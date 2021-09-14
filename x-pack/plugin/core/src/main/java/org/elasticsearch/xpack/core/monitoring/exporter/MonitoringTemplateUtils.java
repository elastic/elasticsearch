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
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.time.Instant;
import java.util.Locale;

public final class MonitoringTemplateUtils {

    private static final String TEMPLATE_FILE = "/monitoring-%s.json";
    private static final String TEMPLATE_VERSION_PROPERTY = "monitoring.template.version";

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

    /**
     * IDs of templates that can be used with {@linkplain #loadTemplate(String) loadTemplate}.
     */
    public static final String[] TEMPLATE_IDS = { "alerts-7", "es", "kibana", "logstash", "beats" };

    /**
     * IDs of templates that can be used with {@linkplain #createEmptyTemplate(String) createEmptyTemplate} that are not managed by a
     * Resolver.
     * <p>
     * These should only be used by the HTTP Exporter to create old templates so that older versions can be properly upgraded. Older
     * instances will attempt to create a named template based on the templates that they expect (e.g., ".monitoring-es-2") and not the
     * ones that we are creating.
     */
    public static final String[] OLD_TEMPLATE_IDS = { "data", "es", "kibana", "logstash" }; //excluding alerts since 6.x watches use it

    private MonitoringTemplateUtils() { }

    /**
     * Get a template name for any template ID.
     *
     * @param id The template identifier.
     * @return Never {@code null} {@link String} prefixed by ".monitoring-".
     * @see #TEMPLATE_IDS
     */
    public static String templateName(final String id) {
        return ".monitoring-" + id;
    }

    /**
     * Get a template name for any template ID for old templates in the previous version.
     *
     * @param id The template identifier.
     * @return Never {@code null} {@link String} prefixed by ".monitoring-" and ended by the {@code OLD_TEMPLATE_VERSION}.
     * @see #OLD_TEMPLATE_IDS
     */
    public static String oldTemplateName(final String id) {
        return ".monitoring-" + id + "-" + OLD_TEMPLATE_VERSION;
    }

    public static String loadTemplate(final String id) {
        String resource = String.format(Locale.ROOT, TEMPLATE_FILE, id);
        return TemplateUtils.loadTemplate(resource, TEMPLATE_VERSION, TEMPLATE_VERSION_PROPERTY);
    }

    /**
     * Create a template that does nothing but exist and provide a newer {@code version} so that we know that <em>we</em> created it.
     *
     * @param id The template identifier.
     * @return Never {@code null}.
     * @see #OLD_TEMPLATE_IDS
     * @see #OLD_TEMPLATE_VERSION
     */
    public static String createEmptyTemplate(final String id) {
        // e.g., { "index_patterns": [ ".monitoring-data-6*" ], "version": 6000002 }
        return "{\"index_patterns\":[\".monitoring-" + id + "-" + OLD_TEMPLATE_VERSION + "*\"],\"version\":" + LAST_UPDATED_VERSION + "}";
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
