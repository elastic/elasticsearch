/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class EqlPluginTests extends ESTestCase {
    public void testEnabledSettingRegisteredInSnapshotBuilds() {
        final EqlPlugin plugin = new EqlPlugin() {

            @Override
            protected boolean isSnapshot() {
                return true;
            }

        };
        assertThat(plugin.getSettings(), hasItem(EqlPlugin.EQL_ENABLED_SETTING));
    }

    public void testEnabledSettingNotRegisteredInNonSnapshotBuilds() {
        final EqlPlugin plugin = new EqlPlugin() {

            @Override
            protected boolean isSnapshot() {
                return false;
            }

        };
        assertThat(plugin.getSettings(), not(hasItem(EqlPlugin.EQL_ENABLED_SETTING)));
    }
}
