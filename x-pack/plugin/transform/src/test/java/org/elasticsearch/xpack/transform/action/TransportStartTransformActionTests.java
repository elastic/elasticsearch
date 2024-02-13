/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;

public class TransportStartTransformActionTests extends ESTestCase {

    // There are 9 combinations of the 2 settings ("skip_dest_index_creation" and "unattended") to test
    public void testShouldSkipDestIndexCreation() {
        SettingsConfig settingsConfig = new SettingsConfig.Builder().build();
        assertFalse(TransportStartTransformAction.shouldSkipDestIndexCreation(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setUnattended(false).build();
        assertFalse(TransportStartTransformAction.shouldSkipDestIndexCreation(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setUnattended(true).build();
        assertTrue(TransportStartTransformAction.shouldSkipDestIndexCreation(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setSkipDestIndexCreation(false).build();
        assertFalse(TransportStartTransformAction.shouldSkipDestIndexCreation(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setSkipDestIndexCreation(false).setUnattended(false).build();
        assertFalse(TransportStartTransformAction.shouldSkipDestIndexCreation(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setSkipDestIndexCreation(false).setUnattended(true).build();
        assertFalse(TransportStartTransformAction.shouldSkipDestIndexCreation(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setSkipDestIndexCreation(true).build();
        assertTrue(TransportStartTransformAction.shouldSkipDestIndexCreation(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setSkipDestIndexCreation(true).setUnattended(false).build();
        assertTrue(TransportStartTransformAction.shouldSkipDestIndexCreation(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setSkipDestIndexCreation(true).setUnattended(true).build();
        assertTrue(TransportStartTransformAction.shouldSkipDestIndexCreation(settingsConfig));
    }
}
