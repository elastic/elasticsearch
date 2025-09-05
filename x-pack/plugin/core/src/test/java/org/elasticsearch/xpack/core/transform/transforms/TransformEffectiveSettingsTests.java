/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;

public class TransformEffectiveSettingsTests extends ESTestCase {

    public void testWriteDatesAsEpochMillis() {
        SettingsConfig settingsConfig = new SettingsConfig.Builder().build();
        assertFalse(TransformEffectiveSettings.writeDatesAsEpochMillis(settingsConfig, TransformConfigVersion.V_7_11_0));
        assertTrue(TransformEffectiveSettings.writeDatesAsEpochMillis(settingsConfig, TransformConfigVersion.V_7_10_1));

        settingsConfig = new SettingsConfig.Builder().setDatesAsEpochMillis(null).build();
        assertFalse(TransformEffectiveSettings.writeDatesAsEpochMillis(settingsConfig, TransformConfigVersion.V_7_11_0));
        // Note that the result is not the same as if we just left "setDatesAsEpochMillis" unset in the builder!
        assertFalse(TransformEffectiveSettings.writeDatesAsEpochMillis(settingsConfig, TransformConfigVersion.V_7_10_1));

        settingsConfig = new SettingsConfig.Builder().setDatesAsEpochMillis(false).build();
        assertFalse(TransformEffectiveSettings.writeDatesAsEpochMillis(settingsConfig, TransformConfigVersion.V_7_11_0));
        assertFalse(TransformEffectiveSettings.writeDatesAsEpochMillis(settingsConfig, TransformConfigVersion.V_7_10_1));

        settingsConfig = new SettingsConfig.Builder().setDatesAsEpochMillis(true).build();
        assertTrue(TransformEffectiveSettings.writeDatesAsEpochMillis(settingsConfig, TransformConfigVersion.V_7_11_0));
        assertTrue(TransformEffectiveSettings.writeDatesAsEpochMillis(settingsConfig, TransformConfigVersion.V_7_10_1));
    }

    public void testIsAlignCheckpointsDisabled() {
        SettingsConfig settingsConfig = new SettingsConfig.Builder().build();
        assertFalse(TransformEffectiveSettings.isAlignCheckpointsDisabled(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setAlignCheckpoints(null).build();
        assertFalse(TransformEffectiveSettings.isAlignCheckpointsDisabled(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setAlignCheckpoints(false).build();
        assertTrue(TransformEffectiveSettings.isAlignCheckpointsDisabled(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setAlignCheckpoints(true).build();
        assertFalse(TransformEffectiveSettings.isAlignCheckpointsDisabled(settingsConfig));
    }

    public void testIsPitDisabled() {
        SettingsConfig settingsConfig = new SettingsConfig.Builder().build();
        assertFalse(TransformEffectiveSettings.isPitDisabled(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setUsePit(null).build();
        assertFalse(TransformEffectiveSettings.isPitDisabled(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setUsePit(false).build();
        assertTrue(TransformEffectiveSettings.isPitDisabled(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setUsePit(true).build();
        assertFalse(TransformEffectiveSettings.isPitDisabled(settingsConfig));
    }

    public void testIsDeduceMappingsDisabled() {
        SettingsConfig settingsConfig = new SettingsConfig.Builder().build();
        assertFalse(TransformEffectiveSettings.isDeduceMappingsDisabled(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setDeduceMappings(null).build();
        assertFalse(TransformEffectiveSettings.isDeduceMappingsDisabled(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setDeduceMappings(false).build();
        assertTrue(TransformEffectiveSettings.isDeduceMappingsDisabled(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setDeduceMappings(true).build();
        assertFalse(TransformEffectiveSettings.isDeduceMappingsDisabled(settingsConfig));
    }

    public void testGetNumFailureRetries() {
        SettingsConfig settingsConfig = new SettingsConfig.Builder().build();
        assertEquals(10, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setNumFailureRetries(null).build();
        assertEquals(10, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setNumFailureRetries(-1).build();
        assertEquals(-1, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setNumFailureRetries(0).build();
        assertEquals(0, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setNumFailureRetries(1).build();
        assertEquals(1, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setNumFailureRetries(10).build();
        assertEquals(10, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setNumFailureRetries(100).build();
        assertEquals(100, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));
    }

    public void testGetNumFailureRetries_Unattended() {
        SettingsConfig settingsConfig = new SettingsConfig.Builder().setUnattended(true).build();
        assertEquals(-1, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setUnattended(true).setNumFailureRetries(null).build();
        assertEquals(-1, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setUnattended(true).setNumFailureRetries(-1).build();
        assertEquals(-1, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setUnattended(true).setNumFailureRetries(0).build();
        assertEquals(-1, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setUnattended(true).setNumFailureRetries(1).build();
        assertEquals(-1, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setUnattended(true).setNumFailureRetries(10).build();
        assertEquals(-1, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));

        settingsConfig = new SettingsConfig.Builder().setUnattended(true).setNumFailureRetries(100).build();
        assertEquals(-1, TransformEffectiveSettings.getNumFailureRetries(settingsConfig, 10));
    }

    public void testIsUnattended() {
        SettingsConfig settingsConfig = new SettingsConfig.Builder().build();
        assertFalse(TransformEffectiveSettings.isUnattended(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setUnattended(null).build();
        assertFalse(TransformEffectiveSettings.isUnattended(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setUnattended(false).build();
        assertFalse(TransformEffectiveSettings.isUnattended(settingsConfig));

        settingsConfig = new SettingsConfig.Builder().setUnattended(true).build();
        assertTrue(TransformEffectiveSettings.isUnattended(settingsConfig));
    }
}
