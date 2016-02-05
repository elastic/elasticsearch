/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.filter;


import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.license.ShieldLicenseState;
import org.elasticsearch.transport.TransportSettings;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.unmodifiableMap;

public class IPFilter {

    /**
     * .http has been chosen for handling HTTP filters, which are not part of the profiles
     * The profiles are only handled for the transport protocol, so we need an own kind of profile
     * for HTTP. This name starts withs a dot, because no profile name can ever start like that due to
     * how we handle settings
     */
    public static final String HTTP_PROFILE_NAME = ".http";

    public static final Setting<Boolean> IP_FILTER_ENABLED_HTTP_SETTING = Setting.boolSetting("shield.http.filter.enabled", true, true,
            Setting.Scope.CLUSTER);
    public static final Setting<Boolean> IP_FILTER_ENABLED_SETTING = new Setting<>("shield.transport.filter.enabled", (s) ->
            IP_FILTER_ENABLED_HTTP_SETTING.getDefaultRaw(s), Booleans::parseBooleanExact, true, Setting.Scope.CLUSTER);
    public static final Setting<List<String>> TRANSPORT_FILTER_ALLOW_SETTING = Setting.listSetting("shield.transport.filter.allow",
            Collections.emptyList(), Function.identity(), true, Setting.Scope.CLUSTER);
    public static final Setting<List<String>> TRANSPORT_FILTER_DENY_SETTING = Setting.listSetting("shield.transport.filter.deny",
            Collections.emptyList(), Function.identity(), true, Setting.Scope.CLUSTER);

    public static final Setting<List<String>> HTTP_FILTER_ALLOW_SETTING = Setting.listSetting("shield.http.filter.allow", (s) -> {
        return Arrays.asList(s.getAsArray("transport.profiles.default.shield.filter.allow",
                TRANSPORT_FILTER_ALLOW_SETTING.get(s).toArray(new String[0])));
    }, Function.identity(), true, Setting.Scope.CLUSTER);
    public static final Setting<List<String>> HTTP_FILTER_DENY_SETTING = Setting.listSetting("shield.http.filter.deny", (s) -> {
        return Arrays.asList(s.getAsArray("transport.profiles.default.shield.filter.deny",
                TRANSPORT_FILTER_DENY_SETTING.get(s).toArray(new String[0])));
    }, Function.identity(), true, Setting.Scope.CLUSTER);


    public static final ShieldIpFilterRule DEFAULT_PROFILE_ACCEPT_ALL = new ShieldIpFilterRule(true, "default:accept_all") {
        @Override
        public boolean contains(InetAddress inetAddress) {
            return true;
        }

        @Override
        public boolean isAllowRule() {
            return true;
        }

        @Override
        public boolean isDenyRule() {
            return false;
        }
    };

    private final AuditTrail auditTrail;
    private final ShieldLicenseState licenseState;
    private final boolean alwaysAllowBoundAddresses;

    private final ESLogger logger;
    private volatile Map<String, ShieldIpFilterRule[]> rules = Collections.emptyMap();
    private volatile boolean isIpFilterEnabled;
    private volatile boolean isHttpFilterEnabled;
    private volatile Map<String, Settings> transportGroups;
    private volatile List<String> transportAllowFilter;
    private volatile List<String> transportDenyFilter;
    private volatile List<String> httpAllowFilter;
    private volatile List<String> httpDenyFilter;
    private final SetOnce<BoundTransportAddress> boundTransportAddress = new SetOnce<>();
    private final SetOnce<BoundTransportAddress> boundHttpTransportAddress = new SetOnce<>();
    private final SetOnce<Map<String, BoundTransportAddress>> profileBoundAddress = new SetOnce<>();

    @Inject
    public IPFilter(final Settings settings, AuditTrail auditTrail, ClusterSettings clusterSettings,
                    ShieldLicenseState licenseState) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.auditTrail = auditTrail;
        this.licenseState = licenseState;
        this.alwaysAllowBoundAddresses = settings.getAsBoolean("shield.filter.always_allow_bound_address", true);
        httpDenyFilter = HTTP_FILTER_DENY_SETTING.get(settings);
        httpAllowFilter = HTTP_FILTER_ALLOW_SETTING.get(settings);
        transportAllowFilter = TRANSPORT_FILTER_ALLOW_SETTING.get(settings);
        transportDenyFilter = TRANSPORT_FILTER_DENY_SETTING.get(settings);
        isHttpFilterEnabled = IP_FILTER_ENABLED_HTTP_SETTING.get(settings);
        isIpFilterEnabled = IP_FILTER_ENABLED_SETTING.get(settings);

        this.transportGroups = TransportSettings.TRANSPORT_PROFILES_SETTING.get(settings).getAsGroups(); // this is pretty crazy that we
        // allow this to be updateable!!! - we have to fix this very soon
        clusterSettings.addSettingsUpdateConsumer(IP_FILTER_ENABLED_HTTP_SETTING, this::setHttpFiltering);
        clusterSettings.addSettingsUpdateConsumer(IP_FILTER_ENABLED_SETTING, this::setTransportFiltering);
        clusterSettings.addSettingsUpdateConsumer(TRANSPORT_FILTER_ALLOW_SETTING, this::setTransportAllowFilter);
        clusterSettings.addSettingsUpdateConsumer(TRANSPORT_FILTER_DENY_SETTING, this::setTransportDenyFilter);
        clusterSettings.addSettingsUpdateConsumer(HTTP_FILTER_ALLOW_SETTING, this::setHttpAllowFilter);
        clusterSettings.addSettingsUpdateConsumer(HTTP_FILTER_DENY_SETTING, this::setHttpDenyFilter);
        clusterSettings.addSettingsUpdateConsumer(TransportSettings.TRANSPORT_PROFILES_SETTING, this::setTransportProfiles);
        updateRules();
    }

    private void setTransportProfiles(Settings settings) {
        transportGroups = settings.getAsGroups();
        updateRules();
    }

    private void setHttpDenyFilter(List<String> filter) {
        this.httpDenyFilter = filter;
        updateRules();
    }

    private void setHttpAllowFilter(List<String> filter) {
        this.httpAllowFilter = filter;
        updateRules();
    }

    private void setTransportDenyFilter(List<String> filter) {
        this.transportDenyFilter = filter;
        updateRules();
    }

    private void setTransportAllowFilter(List<String> filter) {
        this.transportAllowFilter = filter;
        updateRules();
    }

    private void setTransportFiltering(boolean enabled) {
        this.isIpFilterEnabled = enabled;
        updateRules();
    }

    private void setHttpFiltering(boolean enabled) {
        this.isHttpFilterEnabled = enabled;
        updateRules();
    }

    public boolean accept(String profile, InetAddress peerAddress) {
        if (licenseState.securityEnabled() == false) {
            return true;
        }

        if (!rules.containsKey(profile)) {
            // FIXME we need to audit here
            return true;
        }

        for (ShieldIpFilterRule rule : rules.get(profile)) {
            if (rule.contains(peerAddress)) {
                boolean isAllowed = rule.isAllowRule();
                if (isAllowed) {
                    auditTrail.connectionGranted(peerAddress, profile, rule);
                } else {
                    auditTrail.connectionDenied(peerAddress, profile, rule);
                }
                return isAllowed;
            }
        }

        auditTrail.connectionGranted(peerAddress, profile, DEFAULT_PROFILE_ACCEPT_ALL);
        return true;
    }

    private synchronized void updateRules() {
        this.rules = parseSettings();
    }

    private Map<String, ShieldIpFilterRule[]> parseSettings() {
        if (isIpFilterEnabled || isHttpFilterEnabled) {
            Map<String, ShieldIpFilterRule[]> profileRules = new HashMap<>();
            if (isHttpFilterEnabled && boundHttpTransportAddress.get() != null) {
                TransportAddress[] localAddresses = boundHttpTransportAddress.get().boundAddresses();
                profileRules.put(HTTP_PROFILE_NAME, createRules(httpAllowFilter, httpDenyFilter, localAddresses));
            }

            if (isIpFilterEnabled && boundTransportAddress.get() != null) {
                TransportAddress[] localAddresses = boundTransportAddress.get().boundAddresses();
                profileRules.put("default", createRules(transportAllowFilter, transportDenyFilter, localAddresses));
                for (Map.Entry<String, Settings> entry : transportGroups.entrySet()) {
                    String profile = entry.getKey();
                    BoundTransportAddress profileBoundTransportAddress = profileBoundAddress.get().get(profile);
                    if (profileBoundTransportAddress == null) {
                        // this could happen if a user updates the settings dynamically with a new profile
                        logger.warn("skipping ip filter rules for profile [{}] since the profile is not bound to any addresses", profile);
                        continue;
                    }
                    Settings profileSettings = entry.getValue().getByPrefix("shield.filter.");
                    profileRules.put(profile, createRules(Arrays.asList(profileSettings.getAsArray("allow")),
                            Arrays.asList(profileSettings.getAsArray("deny")), profileBoundTransportAddress.boundAddresses()));
                }
            }

            logger.debug("loaded ip filtering profiles: {}", profileRules.keySet());
            return unmodifiableMap(profileRules);
        } else {
            return Collections.emptyMap();
        }

    }

    private ShieldIpFilterRule[] createRules(List<String> allow, List<String> deny, TransportAddress[] boundAddresses) {
        List<ShieldIpFilterRule> rules = new ArrayList<>();
        // if we are always going to allow the bound addresses, then the rule for them should be the first rule in the list
        if (alwaysAllowBoundAddresses) {
            assert boundAddresses != null && boundAddresses.length > 0;
            rules.add(new ShieldIpFilterRule(true, boundAddresses));
        }

        // add all rules to the same list. Allow takes precedence so they must come first!
        for (String value : allow) {
            rules.add(new ShieldIpFilterRule(true, value));
        }
        for (String value : deny) {
            rules.add(new ShieldIpFilterRule(false, value));
        }

        return rules.toArray(new ShieldIpFilterRule[rules.size()]);
    }

    public void setBoundTransportAddress(BoundTransportAddress boundTransportAddress,
                                         Map<String, BoundTransportAddress> profileBoundAddress) {
        this.boundTransportAddress.set(boundTransportAddress);
        this.profileBoundAddress.set(profileBoundAddress);
        updateRules();
    }

    public void setBoundHttpTransportAddress(BoundTransportAddress boundHttpTransportAddress) {
        this.boundHttpTransportAddress.set(boundHttpTransportAddress);
        updateRules();
    }
}
