/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.bootstrap;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class BootstrapElasticPasswordTests extends ESTestCase {

    private ClusterService clusterService;
    private ReservedRealm realm;
    private SecurityLifecycleService lifecycle;
    private ArgumentCaptor<ClusterStateListener> listenerCaptor;
    private ArgumentCaptor<ActionListener> actionLister;

    @Before
    public void setupBootstrap() {
        clusterService = mock(ClusterService.class);
        realm = mock(ReservedRealm.class);
        lifecycle = mock(SecurityLifecycleService.class);
        listenerCaptor = ArgumentCaptor.forClass(ClusterStateListener.class);
        actionLister = ArgumentCaptor.forClass(ActionListener.class);
    }

    public void testNoListenerAttachedWhenNoBootstrapPassword() {
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(Settings.EMPTY, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verifyZeroInteractions(clusterService);
    }

    public void testNoListenerAttachedWhenReservedRealmDisabled() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), randomAlphaOfLength(10));
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verifyZeroInteractions(clusterService);
    }

    public void testPasswordHasToBeValid() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), randomAlphaOfLength(5));
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        expectThrows(ValidationException.class, bootstrap::initiatePasswordBootstrap);

        verifyZeroInteractions(clusterService);
    }

    public void testDoesNotBootstrapUntilStateRecovered() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), randomAlphaOfLength(10));
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verify(clusterService).addListener(listenerCaptor.capture());

        ClusterStateListener listener = listenerCaptor.getValue();

        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        ClusterState state = mock(ClusterState.class);
        ClusterBlocks blocks = mock(ClusterBlocks.class);
        when(event.state()).thenReturn(state);
        when(state.blocks()).thenReturn(blocks);
        when(blocks.hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)).thenReturn(true);
        when(lifecycle.isSecurityIndexOutOfDate()).thenReturn(false);
        when(lifecycle.isSecurityIndexWriteable()).thenReturn(true);

        listener.clusterChanged(event);

        verifyZeroInteractions(realm);
    }

    public void testDoesNotBootstrapUntilSecurityIndexUpdated() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), randomAlphaOfLength(10));
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verify(clusterService).addListener(listenerCaptor.capture());

        ClusterStateListener listener = listenerCaptor.getValue();

        ClusterChangedEvent event = getStateRecoveredEvent();
        when(lifecycle.isSecurityIndexOutOfDate()).thenReturn(true);
        when(lifecycle.isSecurityIndexWriteable()).thenReturn(true);
        when(lifecycle.isSecurityIndexExisting()).thenReturn(true);
        when(lifecycle.isSecurityIndexAvailable()).thenReturn(true);

        listener.clusterChanged(event);

        verifyZeroInteractions(realm);
    }

    public void testDoesNotBootstrapUntilSecurityIndexIfExistingIsAvailable() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), randomAlphaOfLength(10));
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verify(clusterService).addListener(listenerCaptor.capture());

        ClusterStateListener listener = listenerCaptor.getValue();

        ClusterChangedEvent event = getStateRecoveredEvent();
        when(lifecycle.isSecurityIndexOutOfDate()).thenReturn(false);
        when(lifecycle.isSecurityIndexWriteable()).thenReturn(true);
        when(lifecycle.isSecurityIndexExisting()).thenReturn(true);
        when(lifecycle.isSecurityIndexAvailable()).thenReturn(false);

        listener.clusterChanged(event);

        verifyZeroInteractions(realm);
    }

    public void testDoesNotBootstrapUntilSecurityIndexWriteable() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), randomAlphaOfLength(10));
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verify(clusterService).addListener(listenerCaptor.capture());

        ClusterStateListener listener = listenerCaptor.getValue();

        ClusterChangedEvent event = getStateRecoveredEvent();
        when(lifecycle.isSecurityIndexOutOfDate()).thenReturn(false);
        when(lifecycle.isSecurityIndexWriteable()).thenReturn(false);
        when(lifecycle.isSecurityIndexExisting()).thenReturn(true);
        when(lifecycle.isSecurityIndexAvailable()).thenReturn(true);

        listener.clusterChanged(event);

        verifyZeroInteractions(realm);
    }

    public void testDoesAllowBootstrapForUnavailableIndexIfNotExisting() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), randomAlphaOfLength(10));
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verify(clusterService).addListener(listenerCaptor.capture());

        ClusterStateListener listener = listenerCaptor.getValue();

        ClusterChangedEvent event = getStateRecoveredEvent();
        when(lifecycle.isSecurityIndexOutOfDate()).thenReturn(false);
        when(lifecycle.isSecurityIndexWriteable()).thenReturn(true);
        when(lifecycle.isSecurityIndexExisting()).thenReturn(false);
        when(lifecycle.isSecurityIndexAvailable()).thenReturn(false);

        listener.clusterChanged(event);

        verify(realm).bootstrapElasticUserCredentials(any(SecureString.class), any(ActionListener.class));
    }

    public void testDoesNotBootstrapBeginsWhenRecoveryDoneAndIndexReady() {
        String password = randomAlphaOfLength(10);
        ensureBootstrapStarted(password);

        ArgumentCaptor<SecureString> hashedPasswordCaptor = ArgumentCaptor.forClass(SecureString.class);
        verify(realm).bootstrapElasticUserCredentials(hashedPasswordCaptor.capture(), any(ActionListener.class));
        assertTrue(Hasher.BCRYPT.verify(new SecureString(password.toCharArray()), hashedPasswordCaptor.getValue().getChars()));
    }

    public void testWillNotAllowTwoConcurrentBootstrapAttempts() {
        String password = randomAlphaOfLength(10);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), password);
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verify(clusterService).addListener(listenerCaptor.capture());

        ClusterStateListener listener = listenerCaptor.getValue();

        ClusterChangedEvent event = getStateRecoveredEvent();
        when(lifecycle.isSecurityIndexOutOfDate()).thenReturn(false);
        when(lifecycle.isSecurityIndexWriteable()).thenReturn(true);
        when(lifecycle.isSecurityIndexExisting()).thenReturn(true);
        when(lifecycle.isSecurityIndexAvailable()).thenReturn(true);

        listener.clusterChanged(event);
        listener.clusterChanged(event);

        verify(realm, times(1)).bootstrapElasticUserCredentials(any(SecureString.class), any(ActionListener.class));
    }

    public void testWillNotAllowSecondBootstrapAttempt() {
        String password = randomAlphaOfLength(10);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), password);
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verify(clusterService).addListener(listenerCaptor.capture());

        ClusterStateListener listener = listenerCaptor.getValue();

        ClusterChangedEvent event = getStateRecoveredEvent();
        when(lifecycle.isSecurityIndexOutOfDate()).thenReturn(false);
        when(lifecycle.isSecurityIndexWriteable()).thenReturn(true);
        when(lifecycle.isSecurityIndexExisting()).thenReturn(true);
        when(lifecycle.isSecurityIndexAvailable()).thenReturn(true);

        listener.clusterChanged(event);

        verify(realm, times(1)).bootstrapElasticUserCredentials(any(SecureString.class), actionLister.capture());

        actionLister.getValue().onResponse(true);

        listener.clusterChanged(event);

        verify(realm, times(1)).bootstrapElasticUserCredentials(any(SecureString.class), any());
    }

    public void testBootstrapCompleteRemovesListener() {
        String password = randomAlphaOfLength(10);
        ensureBootstrapStarted(password);

        verify(realm).bootstrapElasticUserCredentials(any(SecureString.class), actionLister.capture());

        actionLister.getValue().onResponse(randomBoolean());

        verify(clusterService).removeListener(listenerCaptor.getValue());
    }

    public void testBootstrapFailedRemovesListener() {
        String password = randomAlphaOfLength(10);
        ensureBootstrapStarted(password);

        verify(realm).bootstrapElasticUserCredentials(any(SecureString.class), actionLister.capture());

        actionLister.getValue().onFailure(new RuntimeException("failed"));

        verify(clusterService).removeListener(listenerCaptor.getValue());
    }

    private void ensureBootstrapStarted(String password) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.getKey(), password);
        Settings settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .build();
        BootstrapElasticPassword bootstrap = new BootstrapElasticPassword(settings, logger, clusterService, realm, lifecycle);

        bootstrap.initiatePasswordBootstrap();

        verify(clusterService).addListener(listenerCaptor.capture());

        ClusterStateListener listener = listenerCaptor.getValue();

        ClusterChangedEvent event = getStateRecoveredEvent();
        when(lifecycle.isSecurityIndexOutOfDate()).thenReturn(false);
        when(lifecycle.isSecurityIndexWriteable()).thenReturn(true);
        when(lifecycle.isSecurityIndexExisting()).thenReturn(true);
        when(lifecycle.isSecurityIndexAvailable()).thenReturn(true);

        listener.clusterChanged(event);
    }

    private ClusterChangedEvent getStateRecoveredEvent() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        ClusterState state = mock(ClusterState.class);
        ClusterBlocks blocks = mock(ClusterBlocks.class);
        when(event.state()).thenReturn(state);
        when(state.blocks()).thenReturn(blocks);
        when(blocks.hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)).thenReturn(false);
        return event;
    }
}
