/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class NativeRealmMigratorTests extends ESTestCase {

    private Consumer<ActionListener<Void>> ensureDisabledHandler;
    private Map<String, NativeUsersStore.ReservedUserInfo> reservedUsers;
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
        }).when(nativeUsersStore).ensureReservedUserIsDisabled(anyString(), eq(allowClearCache), any(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            ActionListener<Void> listener = (ActionListener<Void>) invocation.getArguments()[2];
            listener.onResponse(null);
            return null;
        }).when(nativeUsersStore).changePassword(any(ChangePasswordRequest.class), anyBoolean(), any(ActionListener.class));

        reservedUsers = Collections.emptyMap();
        Mockito.doAnswer(invocation -> {
            ActionListener<Map<String, NativeUsersStore.ReservedUserInfo>> listener =
                    (ActionListener<Map<String, NativeUsersStore.ReservedUserInfo>>) invocation.getArguments()[0];
            listener.onResponse(reservedUsers);
            return null;
        }).when(nativeUsersStore).getAllReservedUserInfo(any(ActionListener.class));

        final Settings settings = Settings.EMPTY;

        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthAllowed()).thenReturn(allowClearCache);

        migrator = new NativeRealmMigrator(settings, nativeUsersStore, licenseState);
    }

    public void testNoChangeOnFreshInstall() throws Exception {
        verifyUpgrade(null, false, false);
    }

    public void testNoChangeOnUpgradeAfterV5_3() throws Exception {
        verifyUpgrade(randomFrom(Version.V_6_0_0_alpha1_UNRELEASED), false, false);
    }

    public void testDisableLogstashAndConvertPasswordsOnUpgradeFromVersionPriorToV5_2() throws Exception {
        this.reservedUsers = Collections.singletonMap(
                KibanaUser.NAME,
                new NativeUsersStore.ReservedUserInfo(Hasher.BCRYPT.hash(ReservedRealm.DEFAULT_PASSWORD_TEXT), false, false)
        );
        verifyUpgrade(randomFrom(Version.V_5_1_1_UNRELEASED, Version.V_5_0_2, Version.V_5_0_0), true, true);
    }

    public void testConvertPasswordsOnUpgradeFromVersion5_2() throws Exception {
        this.reservedUsers = randomSubsetOf(randomIntBetween(0, 3), LogstashSystemUser.NAME, KibanaUser.NAME, ElasticUser.NAME)
                .stream().collect(Collectors.toMap(Function.identity(),
                        name -> new NativeUsersStore.ReservedUserInfo(Hasher.BCRYPT.hash(ReservedRealm.DEFAULT_PASSWORD_TEXT),
                                randomBoolean(), false)
                ));
        verifyUpgrade(Version.V_5_2_0_UNRELEASED, false, true);
    }

    public void testExceptionInUsersStoreIsPropagatedToListener() throws Exception {
        final RuntimeException thrown = new RuntimeException("Forced failure");
        this.ensureDisabledHandler = listener -> listener.onFailure(thrown);
        final PlainActionFuture<Boolean> future = doUpgrade(Version.V_5_0_0);
        final ExecutionException caught = expectThrows(ExecutionException.class, future::get);
        assertThat(caught.getCause(), is(thrown));
    }

    private void verifyUpgrade(Version fromVersion, boolean disableLogstashUser, boolean convertDefaultPasswords) throws Exception {
        final PlainActionFuture<Boolean> future = doUpgrade(fromVersion);
        boolean expectedResult = false;
        if (disableLogstashUser) {
            final boolean clearCache = licenseState.isAuthAllowed();
            verify(nativeUsersStore).ensureReservedUserIsDisabled(eq(LogstashSystemUser.NAME), eq(clearCache), any());
            expectedResult = true;
        }
        if (convertDefaultPasswords) {
            verify(nativeUsersStore).getAllReservedUserInfo(any());
            ArgumentCaptor<ChangePasswordRequest> captor = ArgumentCaptor.forClass(ChangePasswordRequest.class);
            verify(nativeUsersStore, times(this.reservedUsers.size()))
                    .changePassword(captor.capture(), eq(true), any(ActionListener.class));
            final List<ChangePasswordRequest> requests = captor.getAllValues();
            this.reservedUsers.keySet().forEach(u -> {
                ChangePasswordRequest request = requests.stream().filter(r -> r.username().equals(u)).findFirst().get();
                assertThat(request.validate(), nullValue(ActionRequestValidationException.class));
                assertThat(request.username(), equalTo(u));
                assertThat(request.passwordHash().length, equalTo(0));
                assertThat(request.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.IMMEDIATE));
            });
            expectedResult = true;
        }
        verifyNoMoreInteractions(nativeUsersStore);
        assertThat(future.get(), is(expectedResult));
    }

    private PlainActionFuture<Boolean> doUpgrade(Version fromVersion) {
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        migrator.performUpgrade(fromVersion, future);
        return future;
    }
}
