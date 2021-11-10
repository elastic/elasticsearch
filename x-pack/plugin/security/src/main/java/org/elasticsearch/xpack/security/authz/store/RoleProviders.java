/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.security.Security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import static org.elasticsearch.core.Map.entry;

/**
 * Encapsulates logic regarding the active set of role providers in the system, and their order
 * The supported providers are (in order):
 *  - built in (reserved) roles
 *  - file-based roles
 *  - index-based roles
 *  - custom (plugin) providers.
 * The set of permitted role providers can change due to changes in the license state.
 */
public class RoleProviders {

    private final List<ChangeListener> changeListeners;

    private final FileRolesStore fileRolesStore;
    private final NativeRolesStore nativeRolesStore;
    private final ReservedRolesStore reservedRolesStore;

    private final Map<String, List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>>> customRoleProviders;

    private final XPackLicenseState licenseState;

    private List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> activeRoleProviders;

    public RoleProviders(
        ReservedRolesStore reservedRolesStore,
        FileRolesStore fileRolesStore,
        NativeRolesStore nativeRolesStore,
        Map<String, List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>>> customRoleProviders,
        XPackLicenseState licenseState
    ) {
        this.changeListeners = new CopyOnWriteArrayList<>();

        this.reservedRolesStore = Objects.requireNonNull(reservedRolesStore);
        this.fileRolesStore = Objects.requireNonNull(fileRolesStore);
        this.fileRolesStore.addListener(this::onRoleModification);
        this.nativeRolesStore = Objects.requireNonNull(nativeRolesStore);
        this.customRoleProviders = Objects.requireNonNull(customRoleProviders);

        this.licenseState = licenseState;
        this.licenseState.addListener(this::onLicenseChange);

        this.activeRoleProviders = calculateActiveRoleProviders();
    }

    private void onLicenseChange() {
        List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> previousProviders = activeRoleProviders;
        activeRoleProviders = calculateActiveRoleProviders();
        if (activeRoleProviders.equals(previousProviders) == false) {
            changeListeners.forEach(ChangeListener::providersChanged);
        }
    }

    private List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> calculateActiveRoleProviders() {
        final List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> builtInRoleProviders = Arrays.asList(
            reservedRolesStore,
            fileRolesStore,
            nativeRolesStore
        );
        if (customRoleProviders.isEmpty()) {
            return builtInRoleProviders;
        }

        final List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> providers = new ArrayList<>();
        providers.addAll(builtInRoleProviders);

        final XPackLicenseState fixedLicenseState = this.licenseState.copyCurrentLicenseState();
        this.customRoleProviders.forEach((name, customProviders) -> {
            if (Security.CUSTOM_ROLE_PROVIDERS_FEATURE.checkAndStartTracking(fixedLicenseState, name)) {
                providers.addAll(customProviders);
            } else {
                Security.CUSTOM_ROLE_PROVIDERS_FEATURE.stopTracking(fixedLicenseState, name);
            }
        });
        return org.elasticsearch.core.List.copyOf(providers);
    }

    private void onRoleModification(Set<String> roles) {
        changeListeners.forEach(l -> l.rolesChanged(roles));
    }

    public void addChangeListener(ChangeListener listener) {
        changeListeners.add(Objects.requireNonNull(listener));
    }

    public List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> getProviders() {
        return this.activeRoleProviders;
    }

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        final Map<String, Object> fileUsage = fileRolesStore.usageStats();
        nativeRolesStore.usageStats(
            listener.map(nativeUsage -> org.elasticsearch.core.Map.ofEntries(entry("file", fileUsage), entry("native", nativeUsage)))
        );
    }

    interface ChangeListener {
        void rolesChanged(Set<String> roles);

        void providersChanged();
    }
}
