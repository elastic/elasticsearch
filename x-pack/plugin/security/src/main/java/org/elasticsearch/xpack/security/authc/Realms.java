/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.security.Security;
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
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Serves as a realms registry (also responsible for ordering the realms appropriately)
 */
public class Realms implements Iterable<Realm> {

    private static final Logger logger = LogManager.getLogger(Realms.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    private final Settings settings;
    private final Environment env;
    private final Map<String, Realm.Factory> factories;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final ReservedRealm reservedRealm;

    // All realms that were configured from the node settings, some of these may not be enabled due to licensing
    private final List<Realm> allConfiguredRealms;

    // a list of realms that are considered native, that is they only interact with x-pack and no 3rd party auth sources
    private final List<Realm> nativeRealmsOnly;

    // the realms in current use. This list will change dynamically as the license changes
    private volatile List<Realm> activeRealms;

    public Realms(
        Settings settings,
        Environment env,
        Map<String, Realm.Factory> factories,
        XPackLicenseState licenseState,
        ThreadContext threadContext,
        ReservedRealm reservedRealm
    ) throws Exception {
        this.settings = settings;
        this.env = env;
        this.factories = factories;
        this.licenseState = licenseState;
        this.threadContext = threadContext;
        this.reservedRealm = reservedRealm;

        assert XPackSettings.SECURITY_ENABLED.get(settings) : "security must be enabled";
        assert factories.get(ReservedRealm.TYPE) == null;

        this.allConfiguredRealms = initRealms();
        this.allConfiguredRealms.forEach(r -> r.initialize(allConfiguredRealms, licenseState));
        assert allConfiguredRealms.get(0) == reservedRealm : "the first realm must be reserved realm";

        this.nativeRealmsOnly = Collections.unmodifiableList(buildFallbackNativeRealms());
        assert nativeRealmsOnly.get(0) == reservedRealm : "the first realm must be reserved realm";

        recomputeActiveRealms();
        licenseState.addListener(this::recomputeActiveRealms);
    }

    private List<Realm> buildFallbackNativeRealms() throws Exception {
        final List<Realm> nativeRealms = new ArrayList<>();
        for (Realm realm : allConfiguredRealms) {
            if (InternalRealms.isBuiltinRealm(realm.type())) {
                nativeRealms.add(realm);
            }
        }
        nativeRealms.add(0, reservedRealm);
        if (nativeRealms.size() == 1) {
            addNativeRealms(nativeRealms);
            nativeRealms.forEach(r -> r.initialize(nativeRealms, licenseState));
        }
        return nativeRealms;
    }

    protected void recomputeActiveRealms() {
        final XPackLicenseState licenseStateSnapshot = licenseState.copyCurrentLicenseState();
        if (licenseStateSnapshot.isSecurityEnabled() == false) {
            activeRealms = Collections.emptyList();
            return;
        }

        final List<Realm> licensedRealms = calculateLicensedRealms(licenseStateSnapshot);

        logger.info(
            "license mode is [{}], currently licensed security realms are [{}]",
            licenseStateSnapshot.getOperationMode().description(),
            Strings.collectionToCommaDelimitedString(licensedRealms)
        );

        // Stop license-tracking for any previously-active realms that are no longer allowed
        if (activeRealms != null) {
            activeRealms.stream().filter(r -> licensedRealms.contains(r) == false).forEach(realm -> {
                if (InternalRealms.isStandardRealm(realm.type())) {
                    Security.STANDARD_REALMS_FEATURE.stopTracking(licenseStateSnapshot, realm.name());
                } else {
                    Security.ALL_REALMS_FEATURE.stopTracking(licenseStateSnapshot, realm.name());
                }
            });
        }

        activeRealms = licensedRealms;
    }

    @Override
    public Iterator<Realm> iterator() {
        return getActiveRealms().iterator();
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
        return Collections.unmodifiableList(
            allConfiguredRealms.stream().filter(r -> activeSnapshot.contains(r) == false).collect(Collectors.toList())
        );
    }

    public Stream<Realm> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public List<Realm> getActiveRealms() {
        assert activeRealms != null : "Active realms not configured";
        return activeRealms;
    }

    // Protected for testing
    protected List<Realm> calculateLicensedRealms(XPackLicenseState licenseStateSnapshot) {
        List<Realm> licensed = allConfiguredRealms.stream().filter(r -> checkLicense(r, licenseStateSnapshot)).collect(Collectors.toList());
        if (hasUserRealm(licensed)) {
            return Collections.unmodifiableList(licensed);
        } else {
            return nativeRealmsOnly;
        }
    }

    /**
     * Returns true if this list contains at least one non-reserved realm
     */
    private boolean hasUserRealm(List<Realm> licensedRealms) {
        if (licensedRealms.isEmpty()) {
            return false;
        }
        if (licensedRealms.size() == 1 && licensedRealms.get(0) == reservedRealm) {
            return false;
        }
        return true;
    }

    private static boolean checkLicense(Realm realm, XPackLicenseState licenseState) {
        if (isBasicLicensedRealm(realm.type())) {
            return true;
        }
        if (InternalRealms.isStandardRealm(realm.type())) {
            return Security.STANDARD_REALMS_FEATURE.checkAndStartTracking(licenseState, realm.name());
        }
        return Security.ALL_REALMS_FEATURE.checkAndStartTracking(licenseState, realm.name());
    }

    public static boolean isRealmTypeAvailable(XPackLicenseState licenseState, String type) {
        if (Security.ALL_REALMS_FEATURE.checkWithoutTracking(licenseState)) {
            return true;
        } else if (Security.STANDARD_REALMS_FEATURE.checkWithoutTracking(licenseState)) {
            return InternalRealms.isStandardRealm(type) || ReservedRealm.TYPE.equals(type);
        } else {
            return isBasicLicensedRealm(type);
        }
    }

    private static boolean isBasicLicensedRealm(String type) {
        return ReservedRealm.TYPE.equals(type) || InternalRealms.isBuiltinRealm(type);
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
        Set<String> missingOrderRealmSettingKeys = new TreeSet<>();
        Map<String, Set<String>> orderToRealmOrderSettingKeys = new HashMap<>();
        List<RealmConfig.RealmIdentifier> reservedPrefixedRealmIdentifiers = new ArrayList<>();
        Set<String> unconfiguredBasicRealms = new HashSet<>(
            org.elasticsearch.core.Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE)
        );
        for (final Map.Entry<RealmConfig.RealmIdentifier, Settings> entry : realmsSettings.entrySet()) {
            final RealmConfig.RealmIdentifier identifier = entry.getKey();
            if (false == entry.getValue().hasValue(RealmSettings.ORDER_SETTING_KEY)) {
                // If the realm is disabled, it is ok to have no order setting. This is only really useful for file/native realm.
                // Because settings of other realms can just be entirely removed.
                if (entry.getValue().getAsBoolean(RealmSettings.ENABLED_SETTING_KEY, true)) {
                    missingOrderRealmSettingKeys.add(RealmSettings.getFullSettingKey(identifier, RealmSettings.ORDER_SETTING));
                }
            } else {
                orderToRealmOrderSettingKeys.computeIfAbsent(entry.getValue().get(RealmSettings.ORDER_SETTING_KEY), k -> new TreeSet<>())
                    .add(RealmSettings.getFullSettingKey(identifier, RealmSettings.ORDER_SETTING));
            }
            Realm.Factory factory = factories.get(identifier.getType());
            if (factory == null) {
                throw new IllegalArgumentException("unknown realm type [" + identifier.getType() + "] for realm [" + identifier + "]");
            }
            if (identifier.getName().startsWith(RealmSettings.RESERVED_REALM_NAME_PREFIX)) {
                reservedPrefixedRealmIdentifiers.add(identifier);
            }
            RealmConfig config = new RealmConfig(identifier, settings, env, threadContext);
            unconfiguredBasicRealms.remove(identifier.getType());
            if (config.enabled() == false) {
                if (logger.isDebugEnabled()) {
                    logger.debug("realm [{}] is disabled", identifier);
                }
                continue;
            }
            if (FileRealmSettings.TYPE.equals(identifier.getType()) || NativeRealmSettings.TYPE.equals(identifier.getType())) {
                // this is an internal realm factory, let's make sure we didn't already registered one
                // (there can only be one instance of an internal realm)
                if (internalTypes.contains(identifier.getType())) {
                    throw new IllegalArgumentException(
                        "multiple ["
                            + identifier.getType()
                            + "] realms are configured. ["
                            + identifier.getType()
                            + "] is an internal realm and therefore there can only be one such realm configured"
                    );
                }
                internalTypes.add(identifier.getType());
            }
            if (KerberosRealmSettings.TYPE.equals(identifier.getType())) {
                kerberosRealmNames.add(identifier.getName());
                if (kerberosRealmNames.size() > 1) {
                    throw new IllegalArgumentException(
                        "multiple realms "
                            + kerberosRealmNames.toString()
                            + " configured of type ["
                            + identifier.getType()
                            + "], ["
                            + identifier.getType()
                            + "] can only have one such realm "
                            + "configured"
                    );
                }
            }
            Realm realm = factory.create(config);
            nameToRealmIdentifier.computeIfAbsent(realm.name(), k -> new HashSet<>())
                .add(RealmSettings.realmSettingPrefix(realm.type()) + realm.name());
            realms.add(realm);
        }

        logDeprecationForImplicitlyDisabledBasicRealms(realms, unconfiguredBasicRealms);
        if (realms.isEmpty() == false) {
            Collections.sort(realms);
        } else {
            // there is no "realms" configuration, add the defaults
            addNativeRealms(realms);
        }
        // always add built in first!
        realms.add(0, reservedRealm);
        String duplicateRealms = nameToRealmIdentifier.entrySet()
            .stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining("; "));
        if (Strings.hasText(duplicateRealms)) {
            throw new IllegalArgumentException("Found multiple realms configured with the same name: " + duplicateRealms + "");
        }

        logDeprecationIfFound(missingOrderRealmSettingKeys, orderToRealmOrderSettingKeys);
        logDeprecationForReservedPrefixedRealmNames(reservedPrefixedRealmIdentifiers);

        return Collections.unmodifiableList(realms);
    }

    @SuppressWarnings("unchecked")
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        final XPackLicenseState licenseStateSnapshot = licenseState.copyCurrentLicenseState();
        Map<String, Object> realmMap = new HashMap<>();
        final AtomicBoolean failed = new AtomicBoolean(false);
        final List<Realm> realmList = getActiveRealms().stream()
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
                        @SuppressWarnings("unchecked")
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
                }, e -> {
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
            RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(
                FileRealmSettings.TYPE,
                FileRealmSettings.DEFAULT_NAME
            );
            realms.add(fileRealm.create(new RealmConfig(realmIdentifier, settings, env, threadContext)));
        }
        Realm.Factory indexRealmFactory = factories.get(NativeRealmSettings.TYPE);
        if (indexRealmFactory != null) {
            RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(
                NativeRealmSettings.TYPE,
                NativeRealmSettings.DEFAULT_NAME
            );
            realms.add(indexRealmFactory.create(new RealmConfig(realmIdentifier, settings, env, threadContext)));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
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

    private void logDeprecationIfFound(Set<String> missingOrderRealmSettingKeys, Map<String, Set<String>> orderToRealmOrderSettingKeys) {
        if (missingOrderRealmSettingKeys.size() > 0) {
            deprecationLogger.deprecate(
                DeprecationCategory.SECURITY,
                "unordered_realm_config",
                "Found realms without order config: [{}]. In next major release, node will fail to start with missing realm order.",
                String.join("; ", missingOrderRealmSettingKeys)
            );
        }
        final List<String> duplicatedRealmOrderSettingKeys = orderToRealmOrderSettingKeys.entrySet()
            .stream()
            .filter(e -> e.getValue().size() > 1)
            .map(e -> e.getKey() + ": " + String.join(",", e.getValue()))
            .sorted()
            .collect(Collectors.toList());
        if (false == duplicatedRealmOrderSettingKeys.isEmpty()) {
            deprecationLogger.deprecate(
                DeprecationCategory.SECURITY,
                "duplicate_realm_order",
                "Found multiple realms configured with the same order: [{}]. "
                    + "In next major release, node will fail to start with duplicated realm order.",
                String.join("; ", duplicatedRealmOrderSettingKeys)
            );
        }
    }

    private void logDeprecationForImplicitlyDisabledBasicRealms(List<Realm> realms, Set<String> unconfiguredBasicRealms) {
        if (realms.isEmpty()) {  // No available realm
            final List<String> explicitlyDisabledBasicRealms = Sets.difference(
                org.elasticsearch.core.Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE),
                unconfiguredBasicRealms
            ).stream().sorted().collect(Collectors.toList());
            if (explicitlyDisabledBasicRealms.isEmpty()) {
                return;
            }
            deprecationLogger.deprecate(
                DeprecationCategory.SECURITY,
                "implicitly_disabled_basic_realms",
                "Found explicitly disabled basic {}: [{}]. But {} will be enabled because no other realms are configured or enabled. "
                    + "In next major release, explicitly disabled basic realms will remain disabled.",
                explicitlyDisabledBasicRealms.size() == 1 ? "realm" : "realms",
                Strings.collectionToDelimitedString(explicitlyDisabledBasicRealms, ","),
                explicitlyDisabledBasicRealms.size() == 1 ? "it" : "they"
            );
        } else { // There are configured and enabled realms
            if (unconfiguredBasicRealms.isEmpty()) {
                return;
            }
            deprecationLogger.deprecate(
                DeprecationCategory.SECURITY,
                "implicitly_disabled_basic_realms",
                "Found implicitly disabled basic {}: [{}]. {} disabled because there are other explicitly configured realms. "
                    + "In next major release, basic realms will always be enabled unless explicitly disabled.",
                unconfiguredBasicRealms.size() == 1 ? "realm" : "realms",
                Strings.collectionToDelimitedString(unconfiguredBasicRealms, ","),
                unconfiguredBasicRealms.size() == 1 ? "It is" : "They are"
            );
        }
    }

    private void logDeprecationForReservedPrefixedRealmNames(List<RealmConfig.RealmIdentifier> realmIdentifiers) {
        if (false == realmIdentifiers.isEmpty()) {
            deprecationLogger.deprecate(
                DeprecationCategory.SECURITY,
                "realm_name_with_reserved_prefix",
                "Found realm "
                    + (realmIdentifiers.size() == 1 ? "name" : "names")
                    + " with reserved prefix [{}]: [{}]. "
                    + "In a future major release, node will fail to start if any realm names start with reserved prefix.",
                RealmSettings.RESERVED_REALM_NAME_PREFIX,
                realmIdentifiers.stream()
                    .map(rid -> RealmSettings.PREFIX + rid.getType() + "." + rid.getName())
                    .sorted()
                    .collect(Collectors.joining("; "))
            );
        }
    }
}
