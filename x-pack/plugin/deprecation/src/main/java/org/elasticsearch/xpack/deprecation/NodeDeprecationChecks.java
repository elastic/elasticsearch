/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.Locale;

class NodeDeprecationChecks {

    static DeprecationIssue checkProcessors(final Settings settings , final PluginsAndModules pluginsAndModules) {
        if (EsExecutors.PROCESSORS_SETTING.exists(settings) == false) {
            return null;
        }
        final String message = String.format(
            Locale.ROOT,
            "setting [%s] is deprecated in favor of setting [%s]",
            EsExecutors.PROCESSORS_SETTING.getKey(),
            EsExecutors.NODE_PROCESSORS_SETTING.getKey());
        final String url =
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-processors";
        final String details = String.format(
            Locale.ROOT,
            "the setting [%s] is currently set to [%d], instead set [%s] to [%d]",
            EsExecutors.PROCESSORS_SETTING.getKey(),
            EsExecutors.PROCESSORS_SETTING.get(settings),
            EsExecutors.NODE_PROCESSORS_SETTING.getKey(),
            EsExecutors.PROCESSORS_SETTING.get(settings));
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details);
    }

}
