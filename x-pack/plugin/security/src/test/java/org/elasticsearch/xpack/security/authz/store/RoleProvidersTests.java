/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.security.Security;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RoleProvidersTests extends ESTestCase {

    public void testFileRoleInvalidationListener() {
        final MockLicenseState licenseState = MockLicenseState.createMock();

        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        AtomicReference<Consumer<Set<String>>> fileChangeListener = new AtomicReference<>(null);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            final Consumer<Set<String>> consumer = (Consumer<Set<String>>) inv.getArguments()[0];
            fileChangeListener.set(consumer);
            return null;
        }).when(fileRolesStore).addListener(any());

        final RoleProviders roleProviders = new RoleProviders(
            mock(ReservedRolesStore.class),
            fileRolesStore,
            mock(NativeRolesStore.class),
            Map.of(),
            licenseState
        );
        assertThat(fileChangeListener.get(), notNullValue());

        final AtomicInteger roleChangeCount = new AtomicInteger(0);
        final AtomicReference<Set<String>> lastRolesChange = new AtomicReference<>(Set.of());

        roleProviders.addChangeListener(new RoleProviders.ChangeListener() {
            @Override
            public void rolesChanged(Set<String> roles) {
                roleChangeCount.incrementAndGet();
                lastRolesChange.set(roles);
            }

            @Override
            public void providersChanged() {
                // ignore
            }
        });

        assertThat(roleProviders.getProviders(), hasSize(3));
        assertThat(roleChangeCount.get(), is(0));

        assertThat(roleProviders.getProviders(), hasSize(3));
        assertThat(roleChangeCount.get(), is(0));

        Set<String> roleNames = Sets.newHashSet(generateRandomStringArray(5, 8, false, false));
        fileChangeListener.get().accept(roleNames);
        assertThat(roleProviders.getProviders(), hasSize(3));
        assertThat(roleChangeCount.get(), is(1));
        assertThat(lastRolesChange.get(), is(roleNames));

        roleNames = Sets.newHashSet(generateRandomStringArray(5, 4, false, false));
        fileChangeListener.get().accept(roleNames);
        assertThat(roleProviders.getProviders(), hasSize(3));
        assertThat(roleChangeCount.get(), is(2));
        assertThat(lastRolesChange.get(), is(roleNames));
    }

    public void testLicenseChange() {
        final MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(Security.CUSTOM_ROLE_PROVIDERS_FEATURE)).thenReturn(true);

        final AtomicReference<LicenseStateListener> licenseListener = new AtomicReference<>(null);
        MockLicenseState.acceptListeners(licenseState, licenseListener::set);

        List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> customProviders = List.of((names, listener) -> {});
        final String extensionName = randomAlphaOfLengthBetween(3, 12);
        final RoleProviders roleProviders = new RoleProviders(
            mock(ReservedRolesStore.class),
            mock(FileRolesStore.class),
            mock(NativeRolesStore.class),
            Map.of(extensionName, customProviders),
            licenseState
        );
        assertThat(licenseListener.get(), notNullValue());

        final AtomicInteger providerChangeCount = new AtomicInteger(0);
        roleProviders.addChangeListener(new RoleProviders.ChangeListener() {
            @Override
            public void rolesChanged(Set<String> roles) {
                // ignore
            }

            @Override
            public void providersChanged() {
                providerChangeCount.incrementAndGet();
            }
        });

        assertThat(roleProviders.getProviders(), hasSize(4));
        assertThat(providerChangeCount.get(), is(0));
        verify(licenseState, times(1)).enableUsageTracking(Security.CUSTOM_ROLE_PROVIDERS_FEATURE, extensionName);

        licenseListener.get().licenseStateChanged();
        assertThat(roleProviders.getProviders(), hasSize(4));
        assertThat(providerChangeCount.get(), is(0)); // no relevant change in license
        verify(licenseState, times(2)).enableUsageTracking(Security.CUSTOM_ROLE_PROVIDERS_FEATURE, extensionName);

        when(licenseState.isAllowed(Security.CUSTOM_ROLE_PROVIDERS_FEATURE)).thenReturn(false);
        licenseListener.get().licenseStateChanged();
        assertThat(roleProviders.getProviders(), hasSize(3));
        assertThat(providerChangeCount.get(), is(1));
        verify(licenseState, times(2)).enableUsageTracking(Security.CUSTOM_ROLE_PROVIDERS_FEATURE, extensionName);
        verify(licenseState, times(1)).disableUsageTracking(Security.CUSTOM_ROLE_PROVIDERS_FEATURE, extensionName);

        when(licenseState.isAllowed(Security.CUSTOM_ROLE_PROVIDERS_FEATURE)).thenReturn(true);
        licenseListener.get().licenseStateChanged();
        assertThat(roleProviders.getProviders(), hasSize(4));
        assertThat(providerChangeCount.get(), is(2));
        verify(licenseState, times(3)).enableUsageTracking(Security.CUSTOM_ROLE_PROVIDERS_FEATURE, extensionName);
        verify(licenseState, times(1)).disableUsageTracking(Security.CUSTOM_ROLE_PROVIDERS_FEATURE, extensionName);
    }

}
