/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Serves as a realms registry (also responsible for ordering the realms appropriately)
 */
public class Realms extends AbstractLifecycleComponent<Realms> implements Iterable<Realm> {

    private final Map<String, Realm.Factory> factories;
    private List<Realm> realms = Collections.EMPTY_LIST;

    @Inject
    public Realms(Settings settings, Map<String, Realm.Factory> factories) {
        super(settings);
        this.factories = factories;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        realms = new CopyOnWriteArrayList<>(initRealms());
    }

    @Override
    protected void doStop() throws ElasticsearchException {}

    @Override
    protected void doClose() throws ElasticsearchException {}

    @Override
    public Iterator<Realm> iterator() {
        return realms.iterator();
    }

    public Realm.Factory realmFactory(String type) {
        return factories.get(type);
    }

    protected List<Realm> initRealms() {
        Settings realmsSettings = componentSettings.getAsSettings("realms");
        Set<String> internalTypes = Sets.newHashSet();
        List<Realm> realms = Lists.newArrayList();
        for (String name : realmsSettings.names()) {
            Settings realmSettings = realmsSettings.getAsSettings(name);
            String type = realmSettings.get("type");
            if (type == null) {
                throw new ShieldSettingsException("Missing realm type for in [" + name + "] realm");
            }
            Realm.Factory factory = factories.get(type);
            if (factory == null) {
                throw new ShieldSettingsException("Unknown realm type [" + type + "] set for realm [" + name + "]");
            }
            if (!realmSettings.getAsBoolean("enabled", true)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("realm [{}] type [{}] is disabled", name, type);
                }
                continue;
            }
            if (factory.internal()) {
                // this is an internal realm factory, let's make sure we didn't already registered one
                // (there can only be one instance of an internal realm)
                if (internalTypes.contains(type)) {
                    throw new ShieldSettingsException("Multiple [" + type + "] realms are configured. [" + type +
                            "] is an internal realm and therefore there can only be one such realm configured");
                }
                internalTypes.add(type);
            }
            realms.add(factory.create(name, realmSettings));
        }

        if (!realms.isEmpty()) {
            CollectionUtil.introSort(realms);
            return realms;
        }

        // there is no "realms" configuration, go over all the factories and try to create defaults
        // for all the internal realms
        realms.add(factories.get(ESUsersRealm.TYPE).createDefault("default_" + ESUsersRealm.TYPE));
        return realms;
    }

    /**
     * returns the settings for the internal realm of the given type. Typically, internal realms may or may
     * not be configured. If they are not configured, they work OOTB using default settings. If they are
     * configured, there can only be one configure for an internal realm.
     */
    public static Settings internalRealmSettings(Settings settings, String realmType) {
        Settings realmsSettings = settings.getComponentSettings(Realms.class).getAsSettings("realms");
        Settings result = null;
        for (String name : realmsSettings.names()) {
            Settings realmSettings = realmsSettings.getAsSettings(name);
            String type = realmSettings.get("type");
            if (type == null) {
                throw new ShieldSettingsException("Missing realm type for in [" + name + "] realm");
            }
            if (type.equals(realmType)) {
                if (result != null) {
                    throw new ShieldSettingsException("Multiple [" + realmType + "] are configured. Only one [" + realmType + "] may be configured");
                }
                result = realmSettings;
            }
        }
        return result != null ? result : ImmutableSettings.EMPTY;
    }

}
