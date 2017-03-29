/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.xpack.template.TemplateUtils;

import java.util.Locale;
import java.util.regex.Pattern;

public final class MonitoringTemplateUtils {

    private static final String TEMPLATE_FILE = "/monitoring-%s.json";
    private static final String TEMPLATE_VERSION_PROPERTY = Pattern.quote("${monitoring.template.version}");

    /** Current version of es and data templates **/
    public static final String TEMPLATE_VERSION = "2";
    /**
     * The name of the non-timestamped data index.
     */
    public static final String DATA_INDEX = ".monitoring-data-" + TEMPLATE_VERSION;
    /**
     * Data types that should be supported by the {@linkplain #DATA_INDEX data index} that were not by the initial release.
     */
    public static final String[] NEW_DATA_TYPES = { "kibana", "logstash", "beats" };

    /**
     * IDs of templates that can be used with {@linkplain #loadTemplate(String) loadTemplate} that are not managed by a Resolver.
     * <p>
     * This will be the complete list of template IDs when resolvers are removed.
     */
    public static final String[] TEMPLATE_IDS = { "alerts" };

    private MonitoringTemplateUtils() {
    }

    /**
     * Get a template name for any template ID.
     *
     * @param id The template identifier.
     * @return Never {@code null} {@link String} prefixed by ".monitoring-" and the
     * @see #TEMPLATE_IDS
     */
    public static String templateName(String id) {
        return ".monitoring-" + id + "-" + TEMPLATE_VERSION;
    }

    public static String loadTemplate(String id) {
        String resource = String.format(Locale.ROOT, TEMPLATE_FILE, id);
        return TemplateUtils.loadTemplate(resource, TEMPLATE_VERSION, TEMPLATE_VERSION_PROPERTY);
    }
}
