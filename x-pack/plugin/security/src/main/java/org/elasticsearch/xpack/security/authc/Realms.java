/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;

import java.util.ArrayList;
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

    // All realms that were explicitly configured in the settings, some of these may not be enabled due to licensing
    private final List<Realm> allConfiguredRealms;
    // the default builtin realms to enable if no other realms are permitted under the current license
    private final List<Realm> fallbackRealms;

    // the realms in current use. This list will change dynamically as the license changes
    private volatile List<Realm> activeRealms;

    public Realms(Settings settings, Environment env, Map<String, Realm.Factory> factories, XPackLicenseState licenseState,
                  ThreadContext threadContext, ReservedRealm reservedRealm) throws Exception {
        this.settings = settings;
        this.env = env;
        this.factories = factories;
        this.licenseState = licenseState;
        this.threadContext = threadContext;
        this.reservedRealm = reservedRealm;
        assert factories.get(ReservedRealm.TYPE) == null;

        this.allConfiguredRealms = initRealms();
        this.fallbackRealms = buildFallbackRealms(reservedRealm);
        this.activeRealms = allConfiguredRealms;

        this.allConfiguredRealms.forEach(r -> r.initialize(allConfiguredRealms, licenseState));
        this.fallbackRealms.stream().filter(r -> r != reservedRealm).forEach(r -> r.initialize(fallbackRealms, licenseState));

        licenseState.addListener(this::recomputeActiveRealms);
        recomputeActiveRealms();
    }

    protected void recomputeActiveRealms() {
        final XPackLicenseState license = licenseState.copyCurrentLicenseState();
        final List<Realm> licensedRealms = calculateLicensedRealms(license);
        if (hasConfigurableRealms(licensedRealms)) {
            activeRealms = licensedRealms;
        } else {
            activeRealms = fallbackRealms;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("license state has changed to [{}]; configured realms are: [{}]; active realms are: [{}]",
                license.getOperationMode(), allConfiguredRealms, activeRealms);
        }
    }

    private static boolean hasConfigurableRealms(List<Realm> realmList) {
        if (realmList.isEmpty()) {
            return false;
        }
        if (realmList.size() == 1 && ReservedRealm.TYPE.equals(realmList.get(0).type())) {
            return false;
        }
        return true;
    }

    // Protected for testing
    protected List<Realm> calculateLicensedRealms(XPackLicenseState license) {
        return allConfiguredRealms.stream()
                .filter(r -> isRealmTypeAvailable(license, r.type()))
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public Iterator<Realm> iterator() {
        return asList().iterator();
    }

    /**
     * Returns a list of realms that are configured, but are not permitted under the current license.
     */
    public List<Realm> getUnlicensedRealms() {
        final List<Realm> activeSnapshot = activeRealms;
        if (activeSnapshot.equals(allConfiguredRealms)) {
            return Collections.emptyList();
        }

        // Otherwise, we return anything in "all realms" that is not in the allowed realm list
        return allConfiguredRealms.stream()
            .filter(r -> activeSnapshot.contains(r) == false)
            .collect(Collectors.toUnmodifiableList());
    }

    public Stream<Realm> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public List<Realm> asList() {
        if (licenseState.isSecurityEnabled() == false) {
            return Collections.emptyList();
        }
        assert activeRealms != null : "Active realms not configured";
        return activeRealms;
    }

    public Realm realm(String name) {
        for (Realm realm : activeRealms) {
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
        Map<String, Set<String>> nameToRealmIdentifier = new HashMap<>();
        Map<Integer, Set<String>> orderToRealmName = new HashMap<>();
        for (RealmConfig.RealmIdentifier identifier : realmsSettings.keySet()) {
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
            Realm realm = factory.create(config);
            nameToRealmIdentifier.computeIfAbsent(realm.name(), k ->
                new HashSet<>()).add(RealmSettings.realmSettingPrefix(realm.type()) + realm.name());
            orderToRealmName.computeIfAbsent(realm.order(), k -> new HashSet<>())
                .add(realm.name());
            realms.add(realm);
        }

        checkUniqueOrders(orderToRealmName);

        if (!realms.isEmpty()) {
            Collections.sort(realms);
        } else {
            // there is no "realms" configuration, add the defaults
            addNativeRealms(realms);
        }
        // always add built in first!
        realms.add(0, reservedRealm);
        String duplicateRealms = nameToRealmIdentifier.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining("; "));
        if (Strings.hasText(duplicateRealms)) {
            throw new IllegalArgumentException("Found multiple realms configured with the same name: " + duplicateRealms + "");
        }
        return Collections.unmodifiableList(realms);
    }

    protected List<Realm> buildFallbackRealms(ReservedRealm reservedRealm) throws Exception {
        List<Realm> fallback = new ArrayList<>(3);
        fallback.add(reservedRealm);
        addNativeRealms(fallback);
        return List.copyOf(fallback);
    }

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        final XPackLicenseState licenseStateSnapshot = licenseState.copyCurrentLicenseState();
        Map<String, Object> realmMap = new HashMap<>();
        final AtomicBoolean failed = new AtomicBoolean(false);
        final List<Realm> realmList = asList().stream()
            .filter(r -> ReservedRealm.TYPE.equals(r.type()) == false)
            .collect(Collectors.toList());
        final Set<String> realmTypes = realmList.stream().map(Realm::type).collect(Collectors.toSet());
        final CountDown countDown = new CountDown(realmList.size());
        final Runnable doCountDown = () -> {
            if ((realmList.isEmpty() || countDown.countDown()) && failed.get() == false) {
                // iterate over the factories so we can add enabled & available info
                for (String type : factories.keySet()) {
                    assert ReservedRealm.TYPE.equals(type) == false;
                    realmMap.compute(type, (key, value) -> {
                        if (value == null) {
                            return MapBuilder.<String, Object>newMapBuilder()
                                .put("enabled", false)
                                .put("available", isRealmTypeAvailable(licenseStateSnapshot, type))
                                .map();
                        }

                        assert value instanceof Map;
                        Map<String, Object> realmTypeUsage = (Map<String, Object>) value;
                        realmTypeUsage.put("enabled", true);
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

    private List<Realm> addNativeRealms(List<Realm> realms) throws Exception {
        Realm.Factory fileRealm = factories.get(FileRealmSettings.TYPE);
        if (fileRealm != null) {
            var realmIdentifier = new RealmConfig.RealmIdentifier(FileRealmSettings.TYPE, "default_" + FileRealmSettings.TYPE);
            realms.add(fileRealm.create(new RealmConfig(
                realmIdentifier,
                ensureOrderSetting(settings, realmIdentifier, Integer.MIN_VALUE + 1),
                env, threadContext)));
        }
        Realm.Factory indexRealmFactory = factories.get(NativeRealmSettings.TYPE);
        if (indexRealmFactory != null) {
            var realmIdentifier = new RealmConfig.RealmIdentifier(NativeRealmSettings.TYPE, "default_" + NativeRealmSettings.TYPE);
            realms.add(indexRealmFactory.create(new RealmConfig(
                realmIdentifier,
                ensureOrderSetting(settings, realmIdentifier, Integer.MIN_VALUE + 2),
                env, threadContext)));
        }
        return realms;
    }

    private Settings ensureOrderSetting(Settings settings, RealmConfig.RealmIdentifier realmIdentifier, int order) {
        String orderSettingKey = RealmSettings.realmSettingPrefix(realmIdentifier) + "order";
        return Settings.builder().put(settings).put(orderSettingKey, order).build();
    }

    private void checkUniqueOrders(Map<Integer, Set<String>> orderToRealmName) {
        String duplicateOrders = orderToRealmName.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining("; "));
        if (Strings.hasText(duplicateOrders)) {
            throw new IllegalArgumentException("Found multiple realms configured with the same order: " + duplicateOrders);
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

    public static boolean isRealmTypeAvailable(XPackLicenseState licenseState, String type) {
        XPackLicenseState.Feature feature = InternalRealms.getLicenseFeature(type);
        boolean allowed = licenseState.checkFeature(feature);
        logger.trace("Realm type [{}] is associated with feature [{}] which {} allowed on license [{}]",
            type, feature, allowed ? "is" : "isn't", licenseState.getOperationMode());
        return allowed;
    }

}
