/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.transport;

import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.junit.Before;

import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DualStackCoordinatorTests extends ESTestCase {

    private DualStackCoordinator coordinator;
    private ClusterSettings clusterSettings;
    private Settings dualStackEnabled = Settings.builder().put(XPackSettings.DUAL_STACK_ENABLED.getKey(), true).build();
    private Settings dualStackDisabled = Settings.builder().put(XPackSettings.DUAL_STACK_ENABLED.getKey(), false).build();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Set<Setting<?>> settingSet = XPackSettings.getAllSettings().stream().filter(Setting::hasNodeScope).collect(Collectors.toSet());
        clusterSettings = new ClusterSettings(Settings.EMPTY, settingSet);
        coordinator = new DualStackCoordinator(clusterSettings);
    }

    public void testEnableAndDisable() {
        assertFalse(coordinator.isDualStackEnabled());
        clusterSettings.applySettings(dualStackEnabled);
        assertTrue(coordinator.isDualStackEnabled());
        clusterSettings.applySettings(dualStackDisabled);
        assertFalse(coordinator.isDualStackEnabled());
    }

    public void testChannelsClosedWhenDisabled() {
        clusterSettings.applySettings(dualStackEnabled);
        assertTrue(coordinator.isDualStackEnabled());

        CloseableChannel channel1 = mock(CloseableChannel.class);
        CloseableChannel channel2 = mock(CloseableChannel.class);
        coordinator.registerPlaintextChannel(channel1);
        coordinator.registerPlaintextChannel(channel2);

        clusterSettings.applySettings(dualStackDisabled);
        verify(channel1).close();
        verify(channel2).close();
    }

    public void testChannelsClosedIfRegisteredWithDisabledCoordinator() {
        assertFalse(coordinator.isDualStackEnabled());

        CloseableChannel channel1 = mock(CloseableChannel.class);
        CloseableChannel channel2 = mock(CloseableChannel.class);
        coordinator.registerPlaintextChannel(channel1);
        coordinator.registerPlaintextChannel(channel2);
        verify(channel1).close();
        verify(channel2).close();
    }
}
