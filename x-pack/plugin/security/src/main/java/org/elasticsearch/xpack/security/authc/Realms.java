/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.DomainConfig;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.support.ReloadableSecurityComponent;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Serves as a realms registry (also responsible for ordering the realms appropriately)
 */
public class Realms extends AbstractLifecycleComponent implements Iterable<Realm>, ReloadableSecurityComponent {

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

    private final Map<String, DomainConfig> domainNameToConfig;
    // The realmRefs include all realms explicitly or implicitly configured regardless whether they are disabled or not
    private final Map<RealmConfig.RealmIdentifier, Authentication.RealmRef> realmRefs;

    // the realms in current use. This list will change dynamically as the license changes
    private volatile List<Realm> activeRealms;

    @SuppressWarnings("this-escape")
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

        final Map<String, DomainConfig> realmToDomainConfig = RealmSettings.computeRealmNameToDomainConfigAssociation(settings);
        domainNameToConfig = realmToDomainConfig.values()
            .stream()
            .distinct()
            .collect(Collectors.toMap(DomainConfig::name, Function.identity()));
        final List<RealmConfig> realmConfigs = buildRealmConfigs();

        // initRealms will add default file and native realm config if they are not explicitly configured
        final List<Realm> initialRealms = initRealms(realmConfigs);
        realmRefs = calculateRealmRefs(realmConfigs, realmToDomainConfig);
        for (Realm realm : initialRealms) {
            Authentication.RealmRef realmRef = Objects.requireNonNull(
                realmRefs.get(new RealmConfig.RealmIdentifier(realm.type(), realm.name())),
                "realmRef can not be null"
            );
            realm.setRealmRef(realmRef);
        }

        this.allConfiguredRealms = initialRealms;
        this.allConfiguredRealms.forEach(r -> r.initialize(this.allConfiguredRealms, licenseState));
        assert this.allConfiguredRealms.get(0) == reservedRealm : "the first realm must be reserved realm";

        this.activeRealms = calculateLicensedRealms(licenseState.copyCurrentLicenseState());
        licenseState.addListener(this::recomputeActiveRealms);
    }

    private Map<RealmConfig.RealmIdentifier, Authentication.RealmRef> calculateRealmRefs(
        Collection<RealmConfig> realmConfigs,
        Map<String, DomainConfig> domainForRealm
    ) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final Map<RealmConfig.RealmIdentifier, Authentication.RealmRef> realmRefs = new HashMap<>();
        assert realmConfigs.stream().noneMatch(rc -> rc.name().equals(reservedRealm.name()) && rc.type().equals(reservedRealm.type()))
            : "reserved realm cannot be configured";
        realmRefs.put(
            new RealmConfig.RealmIdentifier(reservedRealm.type(), reservedRealm.name()),
            new Authentication.RealmRef(reservedRealm.name(), reservedRealm.type(), nodeName, null)
        );

        for (RealmConfig realmConfig : realmConfigs) {
            final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(realmConfig.type(), realmConfig.name());
            final DomainConfig domainConfig = domainForRealm.get(realmConfig.name());
            final RealmDomain realmDomain;
            if (domainConfig != null) {
                final String domainName = domainConfig.name();
                Set<RealmConfig.RealmIdentifier> domainIdentifiers = new HashSet<>();
                for (RealmConfig otherRealmConfig : realmConfigs) {
                    final DomainConfig otherDomainConfig = domainForRealm.get(otherRealmConfig.name());
                    if (otherDomainConfig != null && domainName.equals(otherDomainConfig.name())) {
                        domainIdentifiers.add(otherRealmConfig.identifier());
                    }
                }
                realmDomain = new RealmDomain(domainName, domainIdentifiers);
            } else {
                realmDomain = null;
            }
            realmRefs.put(
                realmIdentifier,
                new Authentication.RealmRef(realmIdentifier.getName(), realmIdentifier.getType(), nodeName, realmDomain)
            );
        }
        assert realmRefs.values().stream().filter(realmRef -> ReservedRealm.TYPE.equals(realmRef.getType())).toList().size() == 1
            : "there must be exactly one reserved realm configured";
        assert realmRefs.values().stream().filter(realmRef -> NativeRealmSettings.TYPE.equals(realmRef.getType())).toList().size() == 1
            : "there must be exactly one native realm configured";
        assert realmRefs.values().stream().filter(realmRef -> FileRealmSettings.TYPE.equals(realmRef.getType())).toList().size() == 1
            : "there must be exactly one file realm configured";
        return Map.copyOf(realmRefs);
    }

    protected void recomputeActiveRealms() {
        final XPackLicenseState licenseStateSnapshot = licenseState.copyCurrentLicenseState();
        final List<Realm> licensedRealms = calculateLicensedRealms(licenseStateSnapshot);
        logger.info(
            "license mode is [{}], currently licensed security realms are [{}]",
            licenseStateSnapshot.getOperationMode().description(),
            Strings.collectionToCommaDelimitedString(licensedRealms)
        );

        // Stop license-tracking for any previously-active realms that are no longer allowed
        if (activeRealms != null) {
            activeRealms.stream().filter(r -> licensedRealms.contains(r) == false).forEach(realm -> {
                handleDisabledRealmDueToLicenseChange(realm, licenseStateSnapshot);
            });
        }

        activeRealms = licensedRealms;
    }

    // Can be overridden in testing
    protected void handleDisabledRealmDueToLicenseChange(Realm realm, XPackLicenseState licenseStateSnapshot) {
        final LicensedFeature.Persistent feature = getLicensedFeatureForRealm(realm.type());
        assert feature != null
            : "Realm ["
                + realm
                + "] with no licensed feature became inactive due to change to license mode ["
                + licenseStateSnapshot.getOperationMode()
                + "]";
        feature.stopTracking(licenseStateSnapshot, realm.name());
        logger.warn(
            "The [{}.{}] realm has been automatically disabled due to a change in license [{}]",
            realm.type(),
            realm.name(),
            licenseStateSnapshot.statusDescription()
        );
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
        return allConfiguredRealms.stream().filter(r -> activeSnapshot.contains(r) == false).toList();
    }

    public Stream<Realm> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public List<Realm> getActiveRealms() {
        assert activeRealms != null : "Active realms not configured";
        return activeRealms;
    }

    public DomainConfig getDomainConfig(String domainName) {
        return domainNameToConfig.get(domainName);
    }

    // Protected for testing
    protected List<Realm> calculateLicensedRealms(XPackLicenseState licenseStateSnapshot) {
        return allConfiguredRealms.stream().filter(r -> checkLicense(r, licenseStateSnapshot)).toList();
    }

    private static boolean checkLicense(Realm realm, XPackLicenseState licenseState) {
        final LicensedFeature.Persistent feature = getLicensedFeatureForRealm(realm.type());
        if (feature == null) {
            return true;
        }
        return feature.checkAndStartTracking(licenseState, realm.name());
    }

    public static boolean isRealmTypeAvailable(XPackLicenseState licenseState, String type) {
        final LicensedFeature.Persistent feature = getLicensedFeatureForRealm(type);
        if (feature == null) {
            return true;
        }
        return feature.checkWithoutTracking(licenseState);
    }

    @Nullable
    private static LicensedFeature.Persistent getLicensedFeatureForRealm(String realmType) {
        assert Strings.hasText(realmType) : "Realm type must be provided (received [" + realmType + "])";
        if (InternalRealms.isInternalRealm(realmType)) {
            return InternalRealms.getLicensedFeature(realmType);
        } else {
            return Security.CUSTOM_REALMS_FEATURE;
        }
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

    protected List<Realm> initRealms(List<RealmConfig> realmConfigs) throws Exception {
        List<Realm> realms = new ArrayList<>();
        Map<String, Set<String>> nameToRealmIdentifier = new HashMap<>();
        Map<Integer, Set<String>> orderToRealmName = new HashMap<>();
        List<RealmConfig.RealmIdentifier> reservedPrefixedRealmIdentifiers = new ArrayList<>();
        for (RealmConfig config : realmConfigs) {
            Realm.Factory factory = factories.get(config.identifier().getType());
            assert factory != null : "unknown realm type [" + config.identifier().getType() + "]";
            if (config.identifier().getName().startsWith(RealmSettings.RESERVED_REALM_AND_DOMAIN_NAME_PREFIX)) {
                reservedPrefixedRealmIdentifiers.add(config.identifier());
            }
            if (config.enabled() == false) {
                if (logger.isDebugEnabled()) {
                    logger.debug("realm [{}] is disabled", config.identifier());
                }
                continue;
            }
            Realm realm = factory.create(config);
            nameToRealmIdentifier.computeIfAbsent(realm.name(), k -> new HashSet<>())
                .add(RealmSettings.realmSettingPrefix(realm.type()) + realm.name());
            orderToRealmName.computeIfAbsent(realm.order(), k -> new HashSet<>()).add(realm.name());
            realms.add(realm);
        }

        checkUniqueOrders(orderToRealmName);
        Collections.sort(realms);
        ensureUniqueExplicitlyConfiguredRealmNames(nameToRealmIdentifier);

        maybeAddBasicRealms(realms, realmConfigs);
        // always add built in first!
        addReservedRealm(realms);
        logDeprecationForReservedPrefixedRealmNames(reservedPrefixedRealmIdentifiers);
        return Collections.unmodifiableList(realms);
    }

    @SuppressWarnings("unchecked")
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        final XPackLicenseState licenseStateSnapshot = licenseState.copyCurrentLicenseState();
        Map<String, Object> realmMap = new HashMap<>();
        final AtomicBoolean failed = new AtomicBoolean(false);
        final List<Realm> realmList = getActiveRealms().stream().filter(r -> ReservedRealm.TYPE.equals(r.type()) == false).toList();
        try (var refs = new RefCountingRunnable(() -> {
            if (failed.get() == false) {
                // iterate over the factories so we can add enabled & available info
                for (String type : factories.keySet()) {
                    assert ReservedRealm.TYPE.equals(type) == false;
                    realmMap.compute(type, (key, value) -> {
                        if (value == null) {
                            return Map.of("enabled", false, "available", isRealmTypeAvailable(licenseStateSnapshot, type));
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
        })) {
            for (Realm realm : realmList) {
                final var ref = refs.acquire();
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
                        ref.close();
                    }
                }, e -> {
                    if (failed.compareAndSet(false, true)) {
                        listener.onFailure(e);
                    }
                }));
            }
        }
    }

    public Map<String, Object> domainUsageStats() {
        if (domainNameToConfig.isEmpty()) {
            return Map.of();
        } else {
            return domainNameToConfig.entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> Map.of("realms", entry.getValue().memberRealmNames())));
        }
    }

    /**
     * Retrieves the {@link Authentication.RealmRef}, which contains the {@link DomainConfig}, if configured,
     * for the passed in {@link RealmConfig.RealmIdentifier}.
     * If the realm is not currently configured, {@code null} is returned.
     */
    public @Nullable Authentication.RealmRef getRealmRef(RealmConfig.RealmIdentifier realmIdentifier) {
        // "file", "native", and "reserved" realms may be renamed, but they refer to the same corpus of users
        if (FileRealmSettings.TYPE.equals(realmIdentifier.getType())) {
            return getFileRealmRef();
        } else if (NativeRealmSettings.TYPE.equals(realmIdentifier.getType())) {
            return getNativeRealmRef();
        } else if (ReservedRealm.TYPE.equals(realmIdentifier.getType())) {
            return getReservedRealmRef();
        } else {
            // but for other realms, it is assumed that a different realm name or realm type signifies a different corpus of users
            return realmRefs.get(realmIdentifier);
        }
    }

    public Authentication.RealmRef getNativeRealmRef() {
        return realmRefs.values()
            .stream()
            .filter(realmRef -> NativeRealmSettings.TYPE.equals(realmRef.getType()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("native realm realm ref not found"));
    }

    public Authentication.RealmRef getFileRealmRef() {
        return realmRefs.values()
            .stream()
            .filter(realmRef -> FileRealmSettings.TYPE.equals(realmRef.getType()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("file realm realm ref not found"));
    }

    public Authentication.RealmRef getReservedRealmRef() {
        return realmRefs.values()
            .stream()
            .filter(realmRef -> ReservedRealm.TYPE.equals(realmRef.getType()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("reserved realm realm ref not found"));
    }

    // should only be useful for testing
    int getRealmRefsCount() {
        return realmRefs.size();
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {
        IOUtils.close(allConfiguredRealms.stream().filter(r -> r instanceof Closeable).map(r -> (Closeable) r).toList());
    }

    private void maybeAddBasicRealms(List<Realm> realms, List<RealmConfig> realmConfigs) throws Exception {
        final Set<String> disabledBasicRealmTypes = findDisabledBasicRealmTypes(realmConfigs);
        final Set<String> realmTypes = realms.stream().map(Realm::type).collect(Collectors.toUnmodifiableSet());
        // Add native realm first so that file realm will be added before it
        if (false == disabledBasicRealmTypes.contains(NativeRealmSettings.TYPE) && false == realmTypes.contains(NativeRealmSettings.TYPE)) {
            boolean enabled = settings.getAsBoolean(NativeRealmSettings.NATIVE_USERS_ENABLED, true);
            ensureRealmNameIsAvailable(realms, NativeRealmSettings.DEFAULT_NAME);
            var nativeRealmId = new RealmConfig.RealmIdentifier(NativeRealmSettings.TYPE, NativeRealmSettings.DEFAULT_NAME);
            var realmConfig = new RealmConfig(
                nativeRealmId,
                buildSettingsforDefaultRealm(settings, nativeRealmId, Integer.MIN_VALUE, enabled),
                env,
                threadContext
            );
            realmConfigs.add(realmConfig);
            if (enabled) {
                realms.add(0, factories.get(NativeRealmSettings.TYPE).create(realmConfig));
            }
        }
        if (false == disabledBasicRealmTypes.contains(FileRealmSettings.TYPE) && false == realmTypes.contains(FileRealmSettings.TYPE)) {
            ensureRealmNameIsAvailable(realms, FileRealmSettings.DEFAULT_NAME);
            var fileRealmId = new RealmConfig.RealmIdentifier(FileRealmSettings.TYPE, FileRealmSettings.DEFAULT_NAME);
            var realmConfig = new RealmConfig(
                fileRealmId,
                buildSettingsforDefaultRealm(settings, fileRealmId, Integer.MIN_VALUE, true),
                env,
                threadContext
            );
            realmConfigs.add(realmConfig);
            realms.add(0, factories.get(FileRealmSettings.TYPE).create(realmConfig));
        }
    }

    private void addReservedRealm(List<Realm> realms) {
        ensureRealmNameIsAvailable(realms, ReservedRealm.NAME);
        realms.add(0, reservedRealm);
    }

    /**
     * Check that the given realmName is not yet used by the given list of realms.
     */
    private static void ensureRealmNameIsAvailable(List<Realm> realms, String realmName) {
        assert realms.size() == realms.stream().map(Realm::name).collect(Collectors.toUnmodifiableSet()).size()
            : "existing realm names must be unique";
        final Realm misNamedRealm = realms.stream().filter(realm -> realmName.equals(realm.name())).findFirst().orElse(null);
        if (misNamedRealm != null) {
            throw new IllegalArgumentException(
                "Found realm configured with name clashing with the ["
                    + realmName
                    + "] realm: ["
                    + (RealmSettings.realmSettingPrefix(misNamedRealm.type()) + misNamedRealm.name())
                    + "]"
            );
        }
    }

    private static Settings buildSettingsforDefaultRealm(
        Settings settings,
        RealmConfig.RealmIdentifier realmIdentifier,
        int order,
        boolean enabled
    ) {
        final String prefix = RealmSettings.realmSettingPrefix(realmIdentifier);
        final Settings.Builder builder = Settings.builder().put(settings).put(prefix + "order", order);
        if (enabled == false) {
            builder.put(prefix + "enabled", false);
        }
        return builder.build();
    }

    private static void checkUniqueOrders(Map<Integer, Set<String>> orderToRealmName) {
        String duplicateOrders = orderToRealmName.entrySet()
            .stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining("; "));
        if (Strings.hasText(duplicateOrders)) {
            throw new IllegalArgumentException("Found multiple realms configured with the same order: " + duplicateOrders);
        }
    }

    private static void ensureUniqueExplicitlyConfiguredRealmNames(Map<String, Set<String>> nameToRealmIdentifier) {
        String duplicateRealms = nameToRealmIdentifier.entrySet()
            .stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining("; "));
        if (Strings.hasText(duplicateRealms)) {
            throw new IllegalArgumentException("Found multiple realms configured with the same name: " + duplicateRealms + "");
        }
    }

    private List<RealmConfig> buildRealmConfigs() {
        final Map<RealmConfig.RealmIdentifier, Settings> realmsSettings = RealmSettings.getRealmSettings(settings);
        final Set<String> internalTypes = new HashSet<>();
        final List<String> kerberosRealmNames = new ArrayList<>();
        final List<RealmConfig> realmConfigs = new ArrayList<>();
        for (RealmConfig.RealmIdentifier identifier : realmsSettings.keySet()) {
            Realm.Factory factory = factories.get(identifier.getType());
            if (factory == null) {
                throw new IllegalArgumentException("unknown realm type [" + identifier.getType() + "] for realm [" + identifier + "]");
            }
            RealmConfig config = new RealmConfig(identifier, settings, env, threadContext);
            if (InternalRealms.isBuiltinRealm(identifier.getType())) {
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
            realmConfigs.add(config);
        }
        return realmConfigs;
    }

    private static Set<String> findDisabledBasicRealmTypes(List<RealmConfig> realmConfigs) {
        return realmConfigs.stream()
            .filter(rc -> InternalRealms.isBuiltinRealm(rc.type()))
            .filter(rc -> false == rc.enabled())
            .map(RealmConfig::type)
            .collect(Collectors.toUnmodifiableSet());
    }

    private static void logDeprecationForReservedPrefixedRealmNames(List<RealmConfig.RealmIdentifier> realmIdentifiers) {
        if (false == realmIdentifiers.isEmpty()) {
            deprecationLogger.warn(
                DeprecationCategory.SECURITY,
                "realm_name_with_reserved_prefix",
                "Found realm "
                    + (realmIdentifiers.size() == 1 ? "name" : "names")
                    + " with reserved prefix [{}]: [{}]. "
                    + "In a future major release, node will fail to start if any realm names start with reserved prefix.",
                RealmSettings.RESERVED_REALM_AND_DOMAIN_NAME_PREFIX,
                realmIdentifiers.stream()
                    .map(rid -> RealmSettings.PREFIX + rid.getType() + "." + rid.getName())
                    .sorted()
                    .collect(Collectors.joining("; "))
            );
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
        Map<String, Object> converted = Maps.newMapWithExpectedSize(map.size());
        for (Entry<String, Object> entry : map.entrySet()) {
            converted.put(entry.getKey(), new ArrayList<>(Collections.singletonList(entry.getValue())));
        }
        return converted;
    }

    @Override
    public void reload(Settings settings) {
        final List<Exception> reloadExceptions = new ArrayList<>();
        for (Realm realm : this.allConfiguredRealms) {
            if (realm instanceof ReloadableSecurityComponent reloadableRealm) {
                try {
                    reloadableRealm.reload(settings);
                } catch (Exception e) {
                    reloadExceptions.add(e);
                }
            }
        }
        if (false == reloadExceptions.isEmpty()) {
            final var combinedException = new ElasticsearchException("secure settings reload failed for one or more realms");
            reloadExceptions.forEach(combinedException::addSuppressed);
            throw combinedException;
        }
    }
}
