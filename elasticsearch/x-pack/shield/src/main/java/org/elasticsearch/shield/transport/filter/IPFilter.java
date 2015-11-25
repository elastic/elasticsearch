/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.filter;

import com.carrotsearch.hppc.ObjectObjectHashMap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.license.ShieldLicenseState;
import org.elasticsearch.transport.Transport;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class IPFilter extends AbstractLifecycleComponent<IPFilter> {

    /**
     * .http has been chosen for handling HTTP filters, which are not part of the profiles
     * The profiles are only handled for the transport protocol, so we need an own kind of profile
     * for HTTP. This name starts withs a dot, because no profile name can ever start like that due to
     * how we handle settings
     */
    public static final String HTTP_PROFILE_NAME = ".http";

    public static final String IP_FILTER_ENABLED_SETTING = "shield.transport.filter.enabled";
    public static final String IP_FILTER_ENABLED_HTTP_SETTING = "shield.http.filter.enabled";

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

    private final LifecycleListener parseSettingsListener = new LifecycleListener() {
        @Override
        public void afterStart() {
            IPFilter.this.rules = IPFilter.this.parseSettings(settings);
        }
    };


    private NodeSettingsService nodeSettingsService;
    private final AuditTrail auditTrail;
    private final Transport transport;
    private final ShieldLicenseState licenseState;
    private final boolean alwaysAllowBoundAddresses;
    private Map<String, ShieldIpFilterRule[]> rules = Collections.emptyMap();
    private HttpServerTransport httpServerTransport = null;

    @Inject
    public IPFilter(final Settings settings, AuditTrail auditTrail, NodeSettingsService nodeSettingsService,
                    Transport transport, ShieldLicenseState licenseState) {
        super(settings);
        this.nodeSettingsService = nodeSettingsService;
        this.auditTrail = auditTrail;
        this.transport = transport;
        this.licenseState = licenseState;
        this.alwaysAllowBoundAddresses = settings.getAsBoolean("shield.filter.always_allow_bound_address", true);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        nodeSettingsService.addListener(new ApplySettings(settings));

        if (transport.lifecycleState() == Lifecycle.State.STARTED) {
            rules = parseSettings(settings);
        } else {
            transport.addLifecycleListener(parseSettingsListener);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    // this cannot be put into the constructor as HTTP might be disabled
    @Inject(optional = true)
    public void setHttpServerTransport(@Nullable HttpServerTransport httpServerTransport) {
        if (httpServerTransport == null) {
            return;
        }

        this.httpServerTransport = httpServerTransport;

        if (httpServerTransport.lifecycleState() == Lifecycle.State.STARTED) {
            IPFilter.this.rules = IPFilter.this.parseSettings(settings);
        } else {
            httpServerTransport.addLifecycleListener(parseSettingsListener);
        }
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

    private Map<String, ShieldIpFilterRule[]> parseSettings(Settings settings) {
        boolean isIpFilterEnabled = settings.getAsBoolean(IP_FILTER_ENABLED_SETTING, true);
        boolean isHttpFilterEnabled = settings.getAsBoolean(IP_FILTER_ENABLED_HTTP_SETTING, isIpFilterEnabled);

        if (!isIpFilterEnabled && !isHttpFilterEnabled) {
            return Collections.emptyMap();
        }

        Map<String, ShieldIpFilterRule[]> profileRules = new HashMap<>();

        if (isHttpFilterEnabled && httpServerTransport != null && httpServerTransport.lifecycleState() == Lifecycle.State.STARTED) {
            TransportAddress[] localAddresses = this.httpServerTransport.boundAddress().boundAddresses();
            String[] httpAllowed = settings.getAsArray("shield.http.filter.allow", settings.getAsArray("transport.profiles.default.shield.filter.allow", settings.getAsArray("shield.transport.filter.allow")));
            String[] httpDenied = settings.getAsArray("shield.http.filter.deny", settings.getAsArray("transport.profiles.default.shield.filter.deny", settings.getAsArray("shield.transport.filter.deny")));
            profileRules.put(HTTP_PROFILE_NAME, createRules(httpAllowed, httpDenied, localAddresses));
        }

        if (isIpFilterEnabled && this.transport.lifecycleState() == Lifecycle.State.STARTED) {
            TransportAddress[] localAddresses = this.transport.boundAddress().boundAddresses();

            String[] allowed = settings.getAsArray("shield.transport.filter.allow");
            String[] denied = settings.getAsArray("shield.transport.filter.deny");
            profileRules.put("default", createRules(allowed, denied, localAddresses));

            Map<String, Settings> groupedSettings = settings.getGroups("transport.profiles.");
            for (Map.Entry<String, Settings> entry : groupedSettings.entrySet()) {
                String profile = entry.getKey();
                BoundTransportAddress profileBoundTransportAddress = transport.profileBoundAddresses().get(profile);
                if (profileBoundTransportAddress == null) {
                    // this could happen if a user updates the settings dynamically with a new profile
                    logger.warn("skipping ip filter rules for profile [{}] since the profile is not bound to any addresses", profile);
                    continue;
                }
                Settings profileSettings = entry.getValue().getByPrefix("shield.filter.");
                profileRules.put(profile, createRules(profileSettings.getAsArray("allow"), profileSettings.getAsArray("deny"), profileBoundTransportAddress.boundAddresses()));
            }
        }

        logger.debug("loaded ip filtering profiles: {}", profileRules.keySet());
        return unmodifiableMap(profileRules);
    }

    private ShieldIpFilterRule[] createRules(String[] allow, String[] deny, TransportAddress[] boundAddresses) {
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

    private class ApplySettings implements NodeSettingsService.Listener {

        String[] allowed;
        String[] denied;
        String[] httpAllowed;
        String[] httpDenied;
        ObjectObjectHashMap<String, String[]> profileAllowed;
        ObjectObjectHashMap<String, String[]> profileDenied;
        private boolean enabled;
        private boolean httpEnabled;

        public ApplySettings(Settings settings) {
            loadValuesFromSettings(settings);
        }

        private void loadValuesFromSettings(Settings settings) {
            this.enabled = settings.getAsBoolean(IP_FILTER_ENABLED_SETTING, this.enabled);
            this.httpEnabled = settings.getAsBoolean(IP_FILTER_ENABLED_HTTP_SETTING, this.httpEnabled);
            this.allowed = settings.getAsArray("shield.transport.filter.allow", this.allowed);
            this.denied = settings.getAsArray("shield.transport.filter.deny", this.denied);
            this.httpAllowed = settings.getAsArray("shield.http.filter.allow", this.httpAllowed);
            this.httpDenied = settings.getAsArray("shield.http.filter.deny", this.httpDenied);

            if (settings.getGroups("transport.profiles.").size() == 0) {
                profileAllowed = HppcMaps.newMap(0);
                profileDenied = HppcMaps.newMap(0);
            }

            profileAllowed = HppcMaps.newNoNullKeysMap(settings.getGroups("transport.profiles.").size());
            profileDenied = HppcMaps.newNoNullKeysMap(settings.getGroups("transport.profiles.").size());
            for (Map.Entry<String, Settings> entry : settings.getGroups("transport.profiles.").entrySet()) {
                profileAllowed.put(entry.getKey(), entry.getValue().getAsArray("shield.filter.allow"));
                profileDenied.put(entry.getKey(), entry.getValue().getAsArray("shield.filter.deny"));
            }
        }

        @Override
        public void onRefreshSettings(Settings settings) {
            if (ipFilterSettingsInvolved(settings) && settingsChanged(settings)) {
                IPFilter.this.rules = parseSettings(settings);
                loadValuesFromSettings(settings);
            }
        }

        private boolean settingsChanged(Settings settings) {
            // simple checks first
            if (this.enabled != settings.getAsBoolean(IP_FILTER_ENABLED_SETTING, this.enabled) ||
                this.httpEnabled != settings.getAsBoolean(IP_FILTER_ENABLED_HTTP_SETTING, this.httpEnabled) ||
                !Arrays.equals(allowed, settings.getAsArray("shield.transport.filter.allow")) ||
                !Arrays.equals(denied, settings.getAsArray("shield.transport.filter.deny")) ||
                !Arrays.equals(httpAllowed, settings.getAsArray("shield.http.filter.allow")) ||
                !Arrays.equals(httpDenied, settings.getAsArray("shield.http.filter.deny"))
                ) {
                return true;
            }

            // profile checks now
            ObjectObjectHashMap<Object, Object> newProfileAllowed = HppcMaps.newNoNullKeysMap(settings.getGroups("transport.profiles.").size());
            ObjectObjectHashMap<Object, Object> newProfileDenied = HppcMaps.newNoNullKeysMap(settings.getGroups("transport.profiles.").size());
            for (Map.Entry<String, Settings> entry : settings.getGroups("transport.profiles.").entrySet()) {
                newProfileAllowed.put(entry.getKey(), entry.getValue().getAsArray("shield.filter.allow"));
                newProfileDenied.put(entry.getKey(), entry.getValue().getAsArray("shield.filter.deny"));
            }

            boolean allowedProfileChanged = !newProfileAllowed.equals(profileAllowed);
            boolean deniedProfileChanged = !newProfileDenied.equals(profileDenied);
            return allowedProfileChanged || deniedProfileChanged;
        }

        private boolean ipFilterSettingsInvolved(Settings settings) {
            boolean containsStaticIpFilterSettings = settings.get("shield.transport.filter.allow") != null ||
                    settings.get("shield.transport.filter.deny") != null ||
                    settings.get("shield.http.filter.allow") != null ||
                    settings.get("shield.http.filter.deny") != null ||
                    settings.get(IP_FILTER_ENABLED_SETTING) != null ||
                    settings.get(IP_FILTER_ENABLED_HTTP_SETTING) != null;

            if (containsStaticIpFilterSettings) {
                return true;
            }

            // now if any profile has a filter setting configured
            for (Map.Entry<String, Settings> entry : settings.getGroups("transport.profiles.").entrySet()) {
                if (entry.getValue().get("shield.filter.allow") != null || entry.getValue().get("shield.filter.deny") != null) {
                    return true;
                }
            }

            return false;
        }
    }
}
