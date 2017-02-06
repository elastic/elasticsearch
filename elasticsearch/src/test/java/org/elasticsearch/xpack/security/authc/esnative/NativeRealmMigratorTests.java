/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;
import org.junit.Before;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class NativeRealmMigratorTests extends ESTestCase {

    private Consumer<ActionListener<Void>> ensureDisabledHandler;
    private NativeUsersStore nativeUsersStore;
    private NativeRealmMigrator migrator;
    private XPackLicenseState licenseState;

    @Before
    public void setupMocks() {
        final boolean allowClearCache = randomBoolean();

        ensureDisabledHandler = listener -> listener.onResponse(null);
        nativeUsersStore = Mockito.mock(NativeUsersStore.class);
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> listener = (ActionListener<Void>) invocation.getArguments()[2];
            ensureDisabledHandler.accept(listener);
            return null;
        }).when(nativeUsersStore).ensureReservedUserIsDisabled(any(), eq(allowClearCache), any());

        final Settings settings = Settings.EMPTY;

        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthAllowed()).thenReturn(allowClearCache);

        migrator = new NativeRealmMigrator(settings, nativeUsersStore, licenseState);
    }

    public void testNoChangeOnFreshInstall() throws Exception {
        verifyNoOpUpgrade(null);
    }

    public void testNoChangeOnUpgradeOnOrAfterV5_2() throws Exception {
        verifyNoOpUpgrade(randomFrom(Version.V_5_2_0_UNRELEASED, Version.V_6_0_0_alpha1_UNRELEASED));
    }

    public void testDisableLogstashOnUpgradeFromVersionPriorToV5_2() throws Exception {
        verifyUpgradeDisablesLogstashSystemUser(randomFrom(Version.V_5_1_1_UNRELEASED, Version.V_5_0_2, Version.V_5_0_0));
    }

    public void testExceptionInUsersStoreIsPropagatedToListener() throws Exception {
        final RuntimeException thrown = new RuntimeException("Forced failure");
        this.ensureDisabledHandler = listener -> listener.onFailure(thrown);
        final PlainActionFuture<Boolean> future = doUpgrade(Version.V_5_0_0);
        final ExecutionException caught = expectThrows(ExecutionException.class, future::get);
        assertThat(caught.getCause(), is(thrown));
    }

    private void verifyNoOpUpgrade(Version fromVersion) throws ExecutionException, InterruptedException {
        final PlainActionFuture<Boolean> future = doUpgrade(fromVersion);
        verifyNoMoreInteractions(nativeUsersStore);
        assertThat(future.get(), is(Boolean.FALSE));
    }

    private void verifyUpgradeDisablesLogstashSystemUser(Version fromVersion) throws ExecutionException, InterruptedException {
        final PlainActionFuture<Boolean> future = doUpgrade(fromVersion);
        final boolean clearCache = licenseState.isAuthAllowed();
        verify(nativeUsersStore).ensureReservedUserIsDisabled(eq(LogstashSystemUser.NAME), eq(clearCache), any());
        verifyNoMoreInteractions(nativeUsersStore);
        assertThat(future.get(), is(Boolean.TRUE));
    }

    private PlainActionFuture<Boolean> doUpgrade(Version fromVersion) {
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        migrator.performUpgrade(fromVersion, future);
        return future;
    }
}