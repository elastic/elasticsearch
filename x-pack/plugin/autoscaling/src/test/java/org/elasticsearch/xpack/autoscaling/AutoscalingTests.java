/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class AutoscalingTests extends ESTestCase {

    public void testEnabledSettingRegisteredInSnapshotBuilds() {
        final Autoscaling plugin = new Autoscaling(Settings.EMPTY) {

            @Override
            protected boolean isSnapshot() {
                return true;
            }

        };
        assertThat(plugin.getSettings(), hasItem(Autoscaling.AUTOSCALING_ENABLED_SETTING));
    }

    public void testEnabledSettingNotRegisteredInNonSnapshotBuilds() {
        final Autoscaling plugin = new Autoscaling(Settings.EMPTY) {

            @Override
            protected boolean isSnapshot() {
                return false;
            }

        };
        assertThat(plugin.getSettings(), not(hasItem(Autoscaling.AUTOSCALING_ENABLED_SETTING)));
    }

}
