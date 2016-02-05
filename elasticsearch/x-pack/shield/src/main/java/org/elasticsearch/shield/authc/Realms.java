/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.esnative.ESNativeRealm;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.license.ShieldLicenseState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Serves as a realms registry (also responsible for ordering the realms appropriately)
 */
public class Realms extends AbstractLifecycleComponent<Realms> implements Iterable<Realm> {

    private final Environment env;
    private final Map<String, Realm.Factory> factories;
    private final ShieldLicenseState shieldLicenseState;

    protected List<Realm> realms = Collections.emptyList();
    // a list of realms that are "internal" in that they are provided by shield and not a third party
    protected List<Realm> internalRealmsOnly = Collections.emptyList();

    @Inject
    public Realms(Settings settings, Environment env, Map<String, Realm.Factory> factories, ShieldLicenseState shieldLicenseState) {
        super(settings);
        this.env = env;
        this.factories = factories;
        this.shieldLicenseState = shieldLicenseState;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        this.realms = initRealms();
        // pre-computing a list of internal only realms allows us to have much cheaper iteration than a custom iterator
        // and is also simpler in terms of logic. These lists are small, so the duplication should not be a real issue here
        List<Realm> internalRealms = new ArrayList<>();
        for (Realm realm : realms) {
            if (AuthenticationModule.INTERNAL_REALM_TYPES.contains(realm.type())) {
                internalRealms.add(realm);
            }
        }

        if (internalRealms.isEmpty()) {
            // lets create a default one so they can do something
            internalRealms.add(factories.get(ESUsersRealm.TYPE).createDefault("default_" + ESUsersRealm.TYPE));
        }
        this.internalRealmsOnly = Collections.unmodifiableList(internalRealms);

    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public Iterator<Realm> iterator() {
        if (shieldLicenseState.customRealmsEnabled()) {
            return realms.iterator();
        }
        return internalRealmsOnly.iterator();
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
        Settings realmsSettings = settings.getAsSettings("shield.authc.realms");
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
            return realms;
        }

        // there is no "realms" configuration, go over all the factories and try to create defaults
        // for all the internal realms
        Realm.Factory indexRealmFactory = factories.get(ESNativeRealm.TYPE);
        if (indexRealmFactory != null) {
            realms.add(indexRealmFactory.createDefault("default_" + ESNativeRealm.TYPE));
        }
        Realm.Factory esUsersRealm = factories.get(ESUsersRealm.TYPE);
        if (esUsersRealm != null) {
            realms.add(esUsersRealm.createDefault("default_" + ESUsersRealm.TYPE));
        }
        return realms;
    }

    /**
     * returns the settings for the internal realm of the given type. Typically, internal realms may or may
     * not be configured. If they are not configured, they work OOTB using default settings. If they are
     * configured, there can only be one configured for an internal realm.
     */
    public static Settings internalRealmSettings(Settings settings, String realmType) {
        Settings realmsSettings = settings.getAsSettings("shield.authc.realms");
        Settings result = null;
        for (String name : realmsSettings.names()) {
            Settings realmSettings = realmsSettings.getAsSettings(name);
            String type = realmSettings.get("type");
            if (type == null) {
                throw new IllegalArgumentException("missing realm type for [" + name + "] realm");
            }
            if (type.equals(realmType)) {
                if (result != null) {
                    throw new IllegalArgumentException("multiple [" + realmType + "] realms are configured. only one [" + realmType +
                            "] may be configured");
                }
                result = realmSettings;
            }
        }
        return result != null ? result : Settings.EMPTY;
    }

}
