/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

/**
 * Node-specific deprecation checks
 */
public class NodeDeprecationChecks {
    static DeprecationIssue javaVersionCheck(Settings nodeSettings, PluginsAndModules plugins) {
        if (JavaVersion.current().compareTo(JavaVersion.parse("11")) < 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Java 11 is required",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html" +
                    "#_java_11_is_required",
                "Java 11 will be required for future versions of Elasticsearch, this node is running version "
                    + JavaVersion.current().toString());
        }
        return null;
    }
}
