/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.MapBuilder;
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
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;

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

/**
 * Serves as a realms registry (also responsible for ordering the realms appropriately)
 */
public class Realms implements Iterable<Realm> {

    private static final Logger logger = LogManager.getLogger(Realms.class);

    private final Settings settings;
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
        this.settings = settings;
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
        realms.forEach(r -> r.initialize(this, licenseState));
    }

    @Override
    public Iterator<Realm> iterator() {
        if (licenseState.isAuthAllowed() == false) {
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

    /**
     * Returns a list of realms that are configured, but are not permitted under the current license.
     */
    public List<Realm> getUnlicensedRealms() {
        // If auth is not allowed, then everything is unlicensed
        if (licenseState.isAuthAllowed() == false) {
            return Collections.unmodifiableList(realms);
        }

        AllowedRealmType allowedRealmType = licenseState.allowedRealmType();
        // If all realms are allowed, then nothing is unlicensed
        if (allowedRealmType == AllowedRealmType.ALL) {
            return Collections.emptyList();
        }

        final List<Realm> allowedRealms = this.asList();
        // Shortcut for the typical case, all the configured realms are allowed
        if (allowedRealms.equals(this.realms)) {
            return Collections.emptyList();
        }

        // Otherwise, we return anything in "all realms" that is not in the allowed realm list
        List<Realm> unlicensed = realms.stream().filter(r -> allowedRealms.contains(r) == false).collect(Collectors.toList());
        return Collections.unmodifiableList(unlicensed);
    }

    public Stream<Realm> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public List<Realm> asList() {
        if (licenseState.isAuthAllowed() == false) {
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
        Map<RealmConfig.RealmIdentifier, Settings> realmsSettings = RealmSettings.getRealmSettings(settings);
        Set<String> internalTypes = new HashSet<>();
        List<Realm> realms = new ArrayList<>();
        List<String> kerberosRealmNames = new ArrayList<>();
        for (RealmConfig.RealmIdentifier identifier: realmsSettings.keySet()) {
            Realm.Factory factory = factories.get(identifier.getType());
            if (factory == null) {
                throw new IllegalArgumentException("unknown realm type [" + identifier.getType() + "] for realm [" + identifier + "]");
            }
            RealmConfig config = new RealmConfig(identifier, settings, env, threadContext);
            if (!config.enabled()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("realm [{}] is disabled", identifier);
                }
                continue;
            }
            if (FileRealmSettings.TYPE.equals(identifier.getType()) || NativeRealmSettings.TYPE.equals(identifier.getType())) {
                // this is an internal realm factory, let's make sure we didn't already registered one
                // (there can only be one instance of an internal realm)
                if (internalTypes.contains(identifier.getType())) {
                    throw new IllegalArgumentException("multiple [" + identifier.getType() + "] realms are configured. ["
                            + identifier.getType() + "] is an internal realm and therefore there can only be one such realm configured");
                }
                internalTypes.add(identifier.getType());
            }
            if (KerberosRealmSettings.TYPE.equals(identifier.getType())) {
                kerberosRealmNames.add(identifier.getName());
                if (kerberosRealmNames.size() > 1) {
                    throw new IllegalArgumentException("multiple realms " + kerberosRealmNames.toString() + " configured of type ["
                        + identifier.getType() + "], [" + identifier.getType() + "] can only have one such realm " +
                        "configured");
                }
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
            realms.add(fileRealm.create(new RealmConfig(
                    new RealmConfig.RealmIdentifier(FileRealmSettings.TYPE, "default_" + FileRealmSettings.TYPE),
                    settings, env, threadContext)));
        }
        Realm.Factory indexRealmFactory = factories.get(NativeRealmSettings.TYPE);
        if (indexRealmFactory != null) {
            realms.add(indexRealmFactory.create(new RealmConfig(
                    new RealmConfig.RealmIdentifier(NativeRealmSettings.TYPE, "default_" + NativeRealmSettings.TYPE),
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
