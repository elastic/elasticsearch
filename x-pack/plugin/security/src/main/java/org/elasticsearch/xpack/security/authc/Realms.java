/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.AllowedRealmType;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;


/**
 * Serves as a realms registry (also responsible for ordering the realms appropriately)
 */
public class Realms extends AbstractComponent implements Iterable<Realm> {

    private final Environment env;
    private final Map<String, Realm.Factory> factories;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final ReservedRealm reservedRealm;

    protected List<Realm> realms;
    // a list of realms that are considered standard in that they are provided by x-pack and
    // interact with a 3rd party source on a limited basis
    List<Realm> standardRealmsOnly;
    // a list of realms that are considered native, that is they only interact with x-pack and no 3rd party auth sources
    List<Realm> nativeRealmsOnly;

    public Realms(Settings settings, Environment env, Map<String, Realm.Factory> factories, XPackLicenseState licenseState,
                  ThreadContext threadContext, ReservedRealm reservedRealm) throws Exception {
        super(settings);
        this.env = env;
        this.factories = factories;
        this.licenseState = licenseState;
        this.threadContext = threadContext;
        this.reservedRealm = reservedRealm;
        assert factories.get(ReservedRealm.TYPE) == null;
        this.realms = initRealms();
        // pre-computing a list of internal only realms allows us to have much cheaper iteration than a custom iterator
        // and is also simpler in terms of logic. These lists are small, so the duplication should not be a real issue here
        List<Realm> standardRealms = new ArrayList<>();
        List<Realm> nativeRealms = new ArrayList<>();
        for (Realm realm : realms) {
            // don't add the reserved realm here otherwise we end up with only this realm...
            if (InternalRealms.isStandardRealm(realm.type())) {
                standardRealms.add(realm);
            }

            if (FileRealmSettings.TYPE.equals(realm.type()) || NativeRealmSettings.TYPE.equals(realm.type())) {
                nativeRealms.add(realm);
            }
        }

        for (List<Realm> realmList : Arrays.asList(standardRealms, nativeRealms)) {
            if (realmList.isEmpty()) {
                addNativeRealms(realmList);
            }

            assert realmList.contains(reservedRealm) == false;
            realmList.add(0, reservedRealm);
            assert realmList.get(0) == reservedRealm;
        }

        this.standardRealmsOnly = Collections.unmodifiableList(standardRealms);
        this.nativeRealmsOnly = Collections.unmodifiableList(nativeRealms);
    }

    @Override
    public Iterator<Realm> iterator() {
        if (licenseState.isSecurityEnabled() == false || licenseState.isAuthAllowed() == false) {
            return Collections.emptyIterator();
        }

        AllowedRealmType allowedRealmType = licenseState.allowedRealmType();
        switch (allowedRealmType) {
            case ALL:
                return realms.iterator();
            case DEFAULT:
                return standardRealmsOnly.iterator();
            case NATIVE:
                return nativeRealmsOnly.iterator();
            default:
                throw new IllegalStateException("authentication should not be enabled");
        }
    }

    public Stream<Realm> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public List<Realm> asList() {
        if (licenseState.isSecurityEnabled() == false || licenseState.isAuthAllowed() == false) {
            return Collections.emptyList();
        }

        AllowedRealmType allowedRealmType = licenseState.allowedRealmType();
        switch (allowedRealmType) {
            case ALL:
                return Collections.unmodifiableList(realms);
            case DEFAULT:
                return Collections.unmodifiableList(standardRealmsOnly);
            case NATIVE:
                return Collections.unmodifiableList(nativeRealmsOnly);
            default:
                throw new IllegalStateException("authentication should not be enabled");
        }
    }

    public Realm realm(String name) {
        for (Realm realm : realms) {
            if (name.equals(realm.name())) {
                return realm;
            }
        }
        return null;
    }

    public Realm.Factory realmFactory(String type) {
        return factories.get(type);
    }

    protected List<Realm> initRealms() throws Exception {
        Settings realmsSettings = RealmSettings.get(settings);
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
            RealmConfig config = new RealmConfig(name, realmSettings, settings, env, threadContext);
            if (!config.enabled()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("realm [{}/{}] is disabled", type, name);
                }
                continue;
            }
            if (FileRealmSettings.TYPE.equals(type) || NativeRealmSettings.TYPE.equals(type)) {
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

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        Map<String, Object> realmMap = new HashMap<>();
        final AtomicBoolean failed = new AtomicBoolean(false);
        final List<Realm> realmList = asList().stream()
            .filter(r -> ReservedRealm.TYPE.equals(r.type()) == false)
            .collect(Collectors.toList());
        final CountDown countDown = new CountDown(realmList.size());
        final Runnable doCountDown = () -> {
            if ((realmList.isEmpty() || countDown.countDown()) && failed.get() == false) {
                final AllowedRealmType allowedRealmType = licenseState.allowedRealmType();
                // iterate over the factories so we can add enabled & available info
                for (String type : factories.keySet()) {
                    assert ReservedRealm.TYPE.equals(type) == false;
                    realmMap.compute(type, (key, value) -> {
                        if (value == null) {
                            return MapBuilder.<String, Object>newMapBuilder()
                                .put("enabled", false)
                                .put("available", isRealmTypeAvailable(allowedRealmType, type))
                                .map();
                        }

                        assert value instanceof Map;
                        Map<String, Object> realmTypeUsage = (Map<String, Object>) value;
                        realmTypeUsage.put("enabled", true);
                        // the realms iterator returned this type so it must be enabled
                        assert isRealmTypeAvailable(allowedRealmType, type);
                        realmTypeUsage.put("available", true);
                        return value;
                    });
                }
                listener.onResponse(realmMap);
            }
        };

        if (realmList.isEmpty()) {
            doCountDown.run();
        } else {
            for (Realm realm : realmList) {
                realm.usageStats(ActionListener.wrap(stats -> {
                        if (failed.get() == false) {
                            synchronized (realmMap) {
                                realmMap.compute(realm.type(), (key, value) -> {
                                    if (value == null) {
                                        Object realmTypeUsage = convertToMapOfLists(stats);
                                        return realmTypeUsage;
                                    }
                                    assert value instanceof Map;
                                    combineMaps((Map<String, Object>) value, stats);
                                    return value;
                                });
                            }
                            doCountDown.run();
                        }
                    },
                    e -> {
                        if (failed.compareAndSet(false, true)) {
                            listener.onFailure(e);
                        }
                    }));
            }
        }
    }

    private void addNativeRealms(List<Realm> realms) throws Exception {
        Realm.Factory fileRealm = factories.get(FileRealmSettings.TYPE);
        if (fileRealm != null) {

            realms.add(fileRealm.create(new RealmConfig("default_" + FileRealmSettings.TYPE, Settings.EMPTY,
                    settings, env, threadContext)));
        }
        Realm.Factory indexRealmFactory = factories.get(NativeRealmSettings.TYPE);
        if (indexRealmFactory != null) {
            realms.add(indexRealmFactory.create(new RealmConfig("default_" + NativeRealmSettings.TYPE, Settings.EMPTY,
                    settings, env, threadContext)));
        }
    }

    private static void combineMaps(Map<String, Object> mapA, Map<String, Object> mapB) {
        for (Entry<String, Object> entry : mapB.entrySet()) {
            mapA.compute(entry.getKey(), (key, value) -> {
                if (value == null) {
                    return new ArrayList<>(Collections.singletonList(entry.getValue()));
                }

                assert value instanceof List;
                ((List) value).add(entry.getValue());
                return value;
            });
        }
    }

    private static Map<String, Object> convertToMapOfLists(Map<String, Object> map) {
        Map<String, Object> converted = new HashMap<>(map.size());
        for (Entry<String, Object> entry : map.entrySet()) {
            converted.put(entry.getKey(), new ArrayList<>(Collections.singletonList(entry.getValue())));
        }
        return converted;
    }

    public static boolean isRealmTypeAvailable(AllowedRealmType enabledRealmType, String type) {
        switch (enabledRealmType) {
            case ALL:
                return true;
            case NONE:
                return false;
            case NATIVE:
                return FileRealmSettings.TYPE.equals(type) || NativeRealmSettings.TYPE.equals(type);
            case DEFAULT:
                return InternalRealms.isStandardRealm(type) || ReservedRealm.TYPE.equals(type);
            default:
                throw new IllegalStateException("unknown enabled realm type [" + enabledRealmType + "]");
        }
    }

}
