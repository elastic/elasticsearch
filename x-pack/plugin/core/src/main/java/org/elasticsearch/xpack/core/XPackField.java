/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

public final class XPackField {
    // These should be moved back to XPackPlugin once its moved to common
    /** Name constant for the security feature. */
    public static final String SECURITY = "security";
    /** Name constant for the monitoring feature. */
    public static final String MONITORING = "monitoring";
    /** Name constant for the watcher feature. */
    public static final String WATCHER = "watcher";
    /** Name constant for the graph feature. */
    public static final String GRAPH = "graph";
    /** Name constant for the machine learning feature. */
    public static final String MACHINE_LEARNING = "ml";
    /** Name constant for the Logstash feature. */
    public static final String LOGSTASH = "logstash";
    /** Name constant for the Beats feature. */
    public static final String BEATS = "beats";
    /** Name constant for the Deprecation API feature. */
    public static final String DEPRECATION = "deprecation";
    /** Name constant for the upgrade feature. */
    public static final String UPGRADE = "upgrade";
    // inside of YAML settings we still use xpack do not having handle issues with dashes
    public static final String SETTINGS_NAME = "xpack";
    /** Name constant for the sql feature. */
    public static final String SQL = "sql";
    /** Name constant for the rollup feature. */
    public static final String ROLLUP = "rollup";
    /** Name constant for the index lifecycle feature. */
    public static final String INDEX_LIFECYCLE = "ilm";

    private XPackField() {}

    public static String featureSettingPrefix(String featureName) {
        return XPackField.SETTINGS_NAME + "." + featureName;
    }
}
