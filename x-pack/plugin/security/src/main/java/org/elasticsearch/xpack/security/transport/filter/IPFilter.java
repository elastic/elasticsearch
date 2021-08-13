/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.filter;


import io.netty.handler.ipfilter.IpFilterRuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;

public class IPFilter {

    /**
     * .http has been chosen for handling HTTP filters, which are not part of the profiles
     * The profiles are only handled for the transport protocol, so we need an own kind of profile
     * for HTTP. This name starts withs a dot, because no profile name can ever start like that due to
     * how we handle settings
     */
    public static final String HTTP_PROFILE_NAME = ".http";

    public static final Setting<Boolean> ALLOW_BOUND_ADDRESSES_SETTING =
            Setting.boolSetting(setting("filter.always_allow_bound_address"), true, Property.NodeScope);

    public static final Setting<Boolean> IP_FILTER_ENABLED_HTTP_SETTING = Setting.boolSetting(setting("http.filter.enabled"),
            true, Property.OperatorDynamic, Property.NodeScope);

    public static final Setting<Boolean> IP_FILTER_ENABLED_SETTING = Setting.boolSetting(setting("transport.filter.enabled"),
            true, Property.OperatorDynamic, Property.NodeScope);

    private static final IPFilterValidator ALLOW_VALIDATOR = new IPFilterValidator(true);
    private static final IPFilterValidator DENY_VALIDATOR = new IPFilterValidator(false);

    public static final Setting<List<String>> TRANSPORT_FILTER_ALLOW_SETTING = Setting.listSetting(
            setting("transport.filter.allow"),
            Collections.emptyList(),
            Function.identity(),
            ALLOW_VALIDATOR,
            Property.OperatorDynamic,
            Property.NodeScope);
    public static final Setting<List<String>> TRANSPORT_FILTER_DENY_SETTING = Setting.listSetting(
            setting("transport.filter.deny"),
            Collections.emptyList(),
            Function.identity(),
            DENY_VALIDATOR,
            Property.OperatorDynamic,
            Property.NodeScope);

    public static final Setting.AffixSetting<List<String>> PROFILE_FILTER_DENY_SETTING = Setting.affixKeySetting(
            "transport.profiles.",
            "xpack.security.filter.deny",
            key -> Setting.listSetting(
                    key,
                    Collections.emptyList(),
                    Function.identity(),
                    DENY_VALIDATOR,
                    Property.OperatorDynamic,
                    Property.NodeScope));
    public static final Setting.AffixSetting<List<String>> PROFILE_FILTER_ALLOW_SETTING = Setting.affixKeySetting(
            "transport.profiles.",
            "xpack.security.filter.allow",
            key -> Setting.listSetting(
                    key,
                    Collections.emptyList(),
                    Function.identity(),
                    ALLOW_VALIDATOR,
                    Property.OperatorDynamic,
                    Property.NodeScope));

    private static final Setting<List<String>> HTTP_FILTER_ALLOW_FALLBACK = Setting.listSetting(
            "transport.profiles.default.xpack.security.filter.allow",
            TRANSPORT_FILTER_ALLOW_SETTING,
            Function.identity(),
            TRANSPORT_FILTER_ALLOW_SETTING::get,
            ALLOW_VALIDATOR,
            Property.NodeScope);
    public static final Setting<List<String>> HTTP_FILTER_ALLOW_SETTING = Setting.listSetting(
            setting("http.filter.allow"),
            HTTP_FILTER_ALLOW_FALLBACK,
            Function.identity(),
            HTTP_FILTER_ALLOW_FALLBACK::get,
            ALLOW_VALIDATOR,
            Property.OperatorDynamic,
            Property.NodeScope);

    private static final Setting<List<String>> HTTP_FILTER_DENY_FALLBACK = Setting.listSetting(
            "transport.profiles.default.xpack.security.filter.deny",
            TRANSPORT_FILTER_DENY_SETTING,
            Function.identity(),
            TRANSPORT_FILTER_DENY_SETTING::get,
            DENY_VALIDATOR,
            Property.NodeScope);
    public static final Setting<List<String>> HTTP_FILTER_DENY_SETTING = Setting.listSetting(
            setting("http.filter.deny"),
            HTTP_FILTER_DENY_FALLBACK,
            Function.identity(),
            HTTP_FILTER_DENY_FALLBACK::get,
            DENY_VALIDATOR,
            Property.OperatorDynamic,
            Property.NodeScope);

    public static final Map<String, Object> DISABLED_USAGE_STATS = Map.of(
            "http", false,
            "transport", false);

    public static final SecurityIpFilterRule DEFAULT_PROFILE_ACCEPT_ALL = new SecurityIpFilterRule(true, "default:accept_all") {

        @Override
        public boolean matches(InetSocketAddress remoteAddress) {
            return true;
        }

        @Override
        public IpFilterRuleType ruleType() {
            return IpFilterRuleType.ACCEPT;
        }
    };

    private static final Logger logger = LogManager.getLogger(IPFilter.class);

    private final AuditTrailService auditTrailService;
    private final XPackLicenseState licenseState;
    private final boolean alwaysAllowBoundAddresses;

    private volatile Map<String, SecurityIpFilterRule[]> rules = Collections.emptyMap();
    private volatile boolean isIpFilterEnabled;
    private volatile boolean isHttpFilterEnabled;
    private final Set<String> profiles;
    private volatile List<String> transportAllowFilter;
    private volatile List<String> transportDenyFilter;
    private volatile List<String> httpAllowFilter;
    private volatile List<String> httpDenyFilter;
    private final SetOnce<BoundTransportAddress> boundTransportAddress = new SetOnce<>();
    private final SetOnce<BoundTransportAddress> boundHttpTransportAddress = new SetOnce<>();
    private final SetOnce<Map<String, BoundTransportAddress>> profileBoundAddress = new SetOnce<>();
    private final Map<String, List<String>> profileAllowRules = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, List<String>> profileDenyRules = Collections.synchronizedMap(new HashMap<>());

    public IPFilter(final Settings settings, AuditTrailService auditTrailService, ClusterSettings clusterSettings,
                    XPackLicenseState licenseState) {
        this.auditTrailService = auditTrailService;
        this.licenseState = licenseState;
        this.alwaysAllowBoundAddresses = ALLOW_BOUND_ADDRESSES_SETTING.get(settings);
        httpDenyFilter = HTTP_FILTER_DENY_SETTING.get(settings);
        httpAllowFilter = HTTP_FILTER_ALLOW_SETTING.get(settings);
        transportAllowFilter = TRANSPORT_FILTER_ALLOW_SETTING.get(settings);
        transportDenyFilter = TRANSPORT_FILTER_DENY_SETTING.get(settings);
        isHttpFilterEnabled = IP_FILTER_ENABLED_HTTP_SETTING.get(settings);
        isIpFilterEnabled = IP_FILTER_ENABLED_SETTING.get(settings);

        this.profiles = settings.getGroups("transport.profiles.",true).keySet().stream().filter(k -> TransportSettings
                .DEFAULT_PROFILE.equals(k) == false).collect(Collectors.toSet()); // exclude default profile -- it's handled differently
        for (String profile : profiles) {
            Setting<List<String>> allowSetting = PROFILE_FILTER_ALLOW_SETTING.getConcreteSettingForNamespace(profile);
            profileAllowRules.put(profile, allowSetting.get(settings));
            Setting<List<String>> denySetting = PROFILE_FILTER_DENY_SETTING.getConcreteSettingForNamespace(profile);
            profileDenyRules.put(profile, denySetting.get(settings));
        }
        clusterSettings.addSettingsUpdateConsumer(IP_FILTER_ENABLED_HTTP_SETTING, this::setHttpFiltering);
        clusterSettings.addSettingsUpdateConsumer(IP_FILTER_ENABLED_SETTING, this::setTransportFiltering);
        clusterSettings.addSettingsUpdateConsumer(TRANSPORT_FILTER_ALLOW_SETTING, this::setTransportAllowFilter);
        clusterSettings.addSettingsUpdateConsumer(TRANSPORT_FILTER_DENY_SETTING, this::setTransportDenyFilter);
        clusterSettings.addSettingsUpdateConsumer(HTTP_FILTER_ALLOW_SETTING, this::setHttpAllowFilter);
        clusterSettings.addSettingsUpdateConsumer(HTTP_FILTER_DENY_SETTING, this::setHttpDenyFilter);
        clusterSettings.addAffixUpdateConsumer(PROFILE_FILTER_ALLOW_SETTING, this::setProfileAllowRules, (a,b) -> {});
        clusterSettings.addAffixUpdateConsumer(PROFILE_FILTER_DENY_SETTING, this::setProfileDenyRules, (a,b) -> {});
        updateRules();
    }

    public Map<String, Object> usageStats() {
        Map<String, Object> map = new HashMap<>(2);
        final boolean httpFilterEnabled = isHttpFilterEnabled && (httpAllowFilter.isEmpty() == false || httpDenyFilter.isEmpty() == false);
        final boolean transportFilterEnabled = isIpFilterEnabled &&
                (transportAllowFilter.isEmpty() == false || transportDenyFilter.isEmpty() == false);
        map.put("http", httpFilterEnabled);
        map.put("transport", transportFilterEnabled);
        return map;
    }

    private void setProfileAllowRules(String profile, List<String> rules) {
        profileAllowRules.put(profile, rules);
        updateRules();
    }

    private void setProfileDenyRules(String profile, List<String> rules) {
        profileDenyRules.put(profile, rules);
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

    public boolean accept(String profile, InetSocketAddress peerAddress) {
        if (Security.IP_FILTERING_FEATURE.check(licenseState) == false) {
            return true;
        }

        if (rules.containsKey(profile) == false) {
            // FIXME we need to audit here
            return true;
        }

        AuditTrail auditTrail = auditTrailService.get();
        for (SecurityIpFilterRule rule : rules.get(profile)) {
            if (rule.matches(peerAddress)) {
                boolean isAllowed = rule.ruleType() == IpFilterRuleType.ACCEPT;
                if (isAllowed) {
                    auditTrail.connectionGranted(peerAddress.getAddress(), profile, rule);
                } else {
                    auditTrail.connectionDenied(peerAddress.getAddress(), profile, rule);
                }
                return isAllowed;
            }
        }

        auditTrail.connectionGranted(peerAddress.getAddress(), profile, DEFAULT_PROFILE_ACCEPT_ALL);
        return true;
    }

    private synchronized void updateRules() {
        this.rules = parseSettings();
    }

    private Map<String, SecurityIpFilterRule[]> parseSettings() {
        if (isIpFilterEnabled || isHttpFilterEnabled) {
            Map<String, SecurityIpFilterRule[]> profileRules = new HashMap<>();
            if (isHttpFilterEnabled && boundHttpTransportAddress.get() != null) {
                TransportAddress[] localAddresses = boundHttpTransportAddress.get().boundAddresses();
                profileRules.put(HTTP_PROFILE_NAME, createRules(httpAllowFilter, httpDenyFilter, localAddresses));
            }

            if (isIpFilterEnabled && boundTransportAddress.get() != null) {
                TransportAddress[] localAddresses = boundTransportAddress.get().boundAddresses();
                profileRules.put(TransportSettings.DEFAULT_PROFILE, createRules(transportAllowFilter, transportDenyFilter, localAddresses));
                for (String profile : profiles) {
                    BoundTransportAddress profileBoundTransportAddress = profileBoundAddress.get().get(profile);
                    if (profileBoundTransportAddress == null) {
                        // this could happen if a user updates the settings dynamically with a new profile
                        logger.warn("skipping ip filter rules for profile [{}] since the profile is not bound to any addresses", profile);
                        continue;
                    }
                    final List<String> allowRules = this.profileAllowRules.getOrDefault(profile, Collections.emptyList());
                    final List<String> denyRules = this.profileDenyRules.getOrDefault(profile, Collections.emptyList());
                    profileRules.put(profile, createRules(allowRules, denyRules, profileBoundTransportAddress.boundAddresses()));
                }
            }

            logger.debug("loaded ip filtering profiles: {}", profileRules.keySet());
            return unmodifiableMap(profileRules);
        } else {
            return Collections.emptyMap();
        }

    }

    private SecurityIpFilterRule[] createRules(List<String> allow, List<String> deny, TransportAddress[] boundAddresses) {
        List<SecurityIpFilterRule> rules = new ArrayList<>();
        // if we are always going to allow the bound addresses, then the rule for them should be the first rule in the list
        if (alwaysAllowBoundAddresses) {
            assert boundAddresses != null && boundAddresses.length > 0;
            rules.add(new SecurityIpFilterRule(true, boundAddresses));
        }

        // add all rules to the same list. Allow takes precedence so they must come first!
        for (String value : allow) {
            rules.add(new SecurityIpFilterRule(true, value));
        }
        for (String value : deny) {
            rules.add(new SecurityIpFilterRule(false, value));
        }

        return rules.toArray(new SecurityIpFilterRule[rules.size()]);
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

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ALLOW_BOUND_ADDRESSES_SETTING);
        settings.add(IP_FILTER_ENABLED_SETTING);
        settings.add(IP_FILTER_ENABLED_HTTP_SETTING);
        settings.add(HTTP_FILTER_ALLOW_SETTING);
        settings.add(HTTP_FILTER_DENY_SETTING);
        settings.add(TRANSPORT_FILTER_ALLOW_SETTING);
        settings.add(TRANSPORT_FILTER_DENY_SETTING);
        settings.add(PROFILE_FILTER_ALLOW_SETTING);
        settings.add(PROFILE_FILTER_DENY_SETTING);
    }

    private static class IPFilterValidator implements Setting.Validator<List<String>> {

        private final boolean isAllowRule;

        IPFilterValidator(boolean isAllowRule) {
            this.isAllowRule = isAllowRule;
        }

        @Override
        public void validate(List<String> filterSpecs) {
            for (String filterSpec : filterSpecs) {
                try {
                    // It's enough just to create the specified rule:
                    new SecurityIpFilterRule(isAllowRule, filterSpec);
                } catch (Exception e) {
                    throw new IllegalArgumentException("invalid IP filter [" + filterSpec + "]", e);
                }
            }
        }
    }

}
