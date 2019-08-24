/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class NodeDeprecationChecksTests extends ESTestCase {

    public void testCheckDefaults() {
        final Settings settings = Settings.EMPTY;
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));
        assertThat(issues, empty());
    }

    public void testCheckPidfile() {
        final String pidfile = randomAlphaOfLength(16);
        final Settings settings = Settings.builder().put(Environment.PIDFILE_SETTING.getKey(), pidfile).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [pidfile] is deprecated in favor of setting [node.pidfile]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-pidfile",
            "the setting [pidfile] is currently set to [" + pidfile + "], instead set [node.pidfile] to [" + pidfile + "]");
        assertThat(issues, contains(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{Environment.PIDFILE_SETTING});
    }

    public void testCheckProcessors() {
        final int processors = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put(EsExecutors.PROCESSORS_SETTING.getKey(), processors).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [processors] is deprecated in favor of setting [node.processors]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-processors",
            "the setting [processors] is currently set to [" + processors + "], instead set [node.processors] to [" + processors + "]");
        assertThat(issues, contains(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{EsExecutors.PROCESSORS_SETTING});
    }

}
