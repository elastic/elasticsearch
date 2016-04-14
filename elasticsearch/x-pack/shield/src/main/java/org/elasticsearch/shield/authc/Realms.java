/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.SecurityLicenseState.EnabledRealmType;
import org.elasticsearch.shield.authc.esnative.ReservedRealm;
import org.elasticsearch.shield.authc.esnative.NativeRealm;
import org.elasticsearch.shield.authc.file.FileRealm;
import org.elasticsearch.shield.SecurityLicenseState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.shield.Security.setting;

/**
 * Serves as a realms registry (also responsible for ordering the realms appropriately)
 */
public class Realms extends AbstractLifecycleComponent<Realms> implements Iterable<Realm> {

    public static final Setting<Settings> REALMS_GROUPS_SETTINGS = Setting.groupSetting(setting("authc.realms."), Property.NodeScope);

    private final Environment env;
    private final Map<String, Realm.Factory> factories;
    private final SecurityLicenseState shieldLicenseState;
    private final ReservedRealm reservedRealm;

    protected List<Realm> realms = Collections.emptyList();
    // a list of realms that are considered default in that they are provided by x-pack and not a third party
    protected List<Realm> internalRealmsOnly = Collections.emptyList();
    // a list of realms that are considered native, that is they only interact with x-pack and no 3rd party auth sources
    protected List<Realm> nativeRealmsOnly = Collections.emptyList();

    @Inject
    public Realms(Settings settings, Environment env, Map<String, Realm.Factory> factories, SecurityLicenseState shieldLicenseState,
                  ReservedRealm reservedRealm) {
        super(settings);
        this.env = env;
        this.factories = factories;
        this.shieldLicenseState = shieldLicenseState;
        this.reservedRealm = reservedRealm;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        assert factories.get(ReservedRealm.TYPE) == null;
        this.realms = initRealms();
        // pre-computing a list of internal only realms allows us to have much cheaper iteration than a custom iterator
        // and is also simpler in terms of logic. These lists are small, so the duplication should not be a real issue here
        List<Realm> internalRealms = new ArrayList<>();
        List<Realm> nativeRealms = new ArrayList<>();
        for (Realm realm : realms) {
            // don't add the reserved realm here otherwise we end up with only this realm...
            if (AuthenticationModule.INTERNAL_REALM_TYPES.contains(realm.type()) && ReservedRealm.TYPE.equals(realm.type()) == false) {
                internalRealms.add(realm);
            }

            if (FileRealm.TYPE.equals(realm.type()) || NativeRealm.TYPE.equals(realm.type())) {
                nativeRealms.add(realm);
            }
        }

        for (List<Realm> realmList : Arrays.asList(internalRealms, nativeRealms)) {
            if (realmList.isEmpty()) {
                addNativeRealms(realmList);
            }

            assert realmList.contains(reservedRealm) == false;
            realmList.add(0, reservedRealm);
            assert realmList.get(0) == reservedRealm;
        }

        this.internalRealmsOnly = Collections.unmodifiableList(internalRealms);
        this.nativeRealmsOnly = Collections.unmodifiableList(nativeRealms);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public Iterator<Realm> iterator() {
        EnabledRealmType enabledRealmType = shieldLicenseState.enabledRealmType();
        switch (enabledRealmType) {
            case ALL:
                return realms.iterator();
            case DEFAULT:
                return internalRealmsOnly.iterator();
            case NATIVE:
                return nativeRealmsOnly.iterator();
            default:
                throw new IllegalStateException("authentication should not be enabled");
        }
    }

    public Realm realm(String name) {
        for (Realm realm : realms) {
            if (name.equals(realm.config.name)) {
                return realm;
            }
        }
        return null;
    }

    public Realm.Factory realmFactory(String type) {
        return factories.get(type);
    }

    protected List<Realm> initRealms() {
        Settings realmsSettings = REALMS_GROUPS_SETTINGS.get(settings);
        Set<String> internalTypes = new HashSet<>();
        List<Realm> realms = new ArrayList<>();
        for (String name : realmsSettings.names()) {
            Settings realmSettings = realmsSettings.getAsSettings(name);
            String type = realmSettings.get("type");
            if (type == null) {
                throw new IllegalArgumentException("missing realm type for [" + name + "] realm");
            }
            Realm.Factory factory = factories.get(type);
            if (factory == null) {
                throw new IllegalArgumentException("unknown realm type [" + type + "] set for realm [" + name + "]");
            }
            RealmConfig config = new RealmConfig(name, realmSettings, settings, env);
            if (!config.enabled()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("realm [{}/{}] is disabled", type, name);
                }
                continue;
            }
            if (factory.internal()) {
                // this is an internal realm factory, let's make sure we didn't already registered one
                // (there can only be one instance of an internal realm)
                if (internalTypes.contains(type)) {
                    throw new IllegalArgumentException("multiple [" + type + "] realms are configured. [" + type +
                            "] is an internal realm and therefore there can only be one such realm configured");
                }
                internalTypes.add(type);
            }
            realms.add(factory.create(config));
        }

        if (!realms.isEmpty()) {
            Collections.sort(realms);
        } else {
            // there is no "realms" configuration, add the defaults
            addNativeRealms(realms);
        }
        // always add built in first!
        realms.add(0, reservedRealm);
        return realms;
    }

    /**
     * returns the settings for the {@link FileRealm}. Typically, this realms may or may
     * not be configured. If it is not configured, it will work OOTB using default settings. If it is
     * configured, there can only be one configured instance.
     */
    public static Settings fileRealmSettings(Settings settings) {
        Settings realmsSettings = REALMS_GROUPS_SETTINGS.get(settings);
        Settings result = null;
        for (String name : realmsSettings.names()) {
            Settings realmSettings = realmsSettings.getAsSettings(name);
            String type = realmSettings.get("type");
            if (type == null) {
                throw new IllegalArgumentException("missing realm type for [" + name + "] realm");
            }
            if (FileRealm.TYPE.equals(type)) {
                if (result != null) {
                    throw new IllegalArgumentException("multiple [" + FileRealm.TYPE +
                            "]realms are configured. only one may be configured");
                }
                result = realmSettings;
            }
        }
        return result != null ? result : Settings.EMPTY;
    }

    private void addNativeRealms(List<Realm> realms) {
        Realm.Factory fileRealm = factories.get(FileRealm.TYPE);
        if (fileRealm != null) {
            realms.add(fileRealm.createDefault("default_" + FileRealm.TYPE));
        }
        Realm.Factory indexRealmFactory = factories.get(NativeRealm.TYPE);
        if (indexRealmFactory != null) {
            realms.add(indexRealmFactory.createDefault("default_" + NativeRealm.TYPE));
        }
    }

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(REALMS_GROUPS_SETTINGS);
    }
}
