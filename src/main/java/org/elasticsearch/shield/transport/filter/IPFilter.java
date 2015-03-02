/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.filter;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.ObjectArrays;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.hppc.ObjectObjectOpenHashMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.transport.Transport;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

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
    private Map<String, ShieldIpFilterRule[]> rules = Collections.EMPTY_MAP;
    private HttpServerTransport httpServerTransport = null;

    @Inject
    public IPFilter(final Settings settings, AuditTrail auditTrail, NodeSettingsService nodeSettingsService, Transport transport) {
        super(settings);
        this.nodeSettingsService = nodeSettingsService;
        this.auditTrail = auditTrail;
        this.transport = transport;
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
        if (!rules.containsKey(profile)) {
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
            return Collections.EMPTY_MAP;
        }

        Map<String, ShieldIpFilterRule[]> profileRules = Maps.newHashMap();

        if (isHttpFilterEnabled && httpServerTransport != null && httpServerTransport.lifecycleState() == Lifecycle.State.STARTED) {
            InetAddress localAddress = ((InetSocketTransportAddress) this.httpServerTransport.boundAddress().boundAddress()).address().getAddress();
            String[] httpAllowed = settings.getAsArray("shield.http.filter.allow", settings.getAsArray("transport.profiles.default.shield.filter.allow", settings.getAsArray("shield.transport.filter.allow")));
            String[] httpDdenied = settings.getAsArray("shield.http.filter.deny", settings.getAsArray("transport.profiles.default.shield.filter.deny", settings.getAsArray("shield.transport.filter.deny")));
            profileRules.put(HTTP_PROFILE_NAME, ObjectArrays.concat(parseValue(httpAllowed, true, localAddress), parseValue(httpDdenied, false, localAddress), ShieldIpFilterRule.class));
        }

        if (isIpFilterEnabled && this.transport.lifecycleState() == Lifecycle.State.STARTED) {
            InetAddress localAddress = ((InetSocketTransportAddress) this.transport.boundAddress().boundAddress()).address().getAddress();

            String[] allowed = settings.getAsArray("shield.transport.filter.allow");
            String[] denied = settings.getAsArray("shield.transport.filter.deny");
            profileRules.put("default", ObjectArrays.concat(parseValue(allowed, true, localAddress), parseValue(denied, false, localAddress), ShieldIpFilterRule.class));

            Map<String, Settings> groupedSettings = settings.getGroups("transport.profiles.");
            for (Map.Entry<String, Settings> entry : groupedSettings.entrySet()) {
                String profile = entry.getKey();
                Settings profileSettings = entry.getValue().getByPrefix("shield.filter.");
                profileRules.put(profile, ObjectArrays.concat(
                        parseValue(profileSettings.getAsArray("allow"), true, localAddress),
                        parseValue(profileSettings.getAsArray("deny"), false, localAddress),
                        ShieldIpFilterRule.class));
            }
        }

        logger.debug("loaded ip filtering profiles: {}", profileRules.keySet());
        return ImmutableMap.copyOf(profileRules);
    }

    private ShieldIpFilterRule[] parseValue(String[] values, boolean isAllowRule, InetAddress localAddress) {
        List<ShieldIpFilterRule> rules = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {

            // never ever deny on localhost, do not even add this rule
            if (!isAllowRule && isLocalAddress(localAddress, values[i])) {
                logger.warn("Configuration setting not applied to reject connections on [{}]. local connections are always allowed!", values[i]);
                continue;
            }

            rules.add(new ShieldIpFilterRule(isAllowRule, values[i]));
        }
        return rules.toArray(new ShieldIpFilterRule[]{});
    }

    private boolean isLocalAddress(InetAddress localAddress, String address) {
        return address.equals("127.0.0.1") || address.equals("localhost") || address.equals("::1") || address.startsWith("fe80::1") ||
               address.equals(localAddress.getHostAddress()) || address.equals(localAddress.getHostName());
    }

    private class ApplySettings implements NodeSettingsService.Listener {

        String[] allowed;
        String[] denied;
        String[] httpAllowed;
        String[] httpDenied;
        ObjectObjectOpenHashMap<String, String[]> profileAllowed;
        ObjectObjectOpenHashMap<String, String[]> profileDenied;
        private boolean enabled;
        private boolean httpEnabled;

        public ApplySettings(Settings settings) {
            loadValuesFromSettings(settings);
        }

        private void loadValuesFromSettings(Settings settings) {
            this.enabled = settings.getAsBoolean(IP_FILTER_ENABLED_SETTING, this.enabled);
            this.httpEnabled = settings.getAsBoolean(IP_FILTER_ENABLED_HTTP_SETTING, this.httpEnabled);
            this.allowed = settings.getAsArray("shield.transport.filter.allow");
            this.denied = settings.getAsArray("shield.transport.filter.deny");
            this.httpAllowed = settings.getAsArray("shield.http.filter.allow");
            this.httpDenied = settings.getAsArray("shield.http.filter.deny");

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
            ObjectObjectOpenHashMap<Object, Object> newProfileAllowed = HppcMaps.newNoNullKeysMap(settings.getGroups("transport.profiles.").size());
            ObjectObjectOpenHashMap<Object, Object> newProfileDenied = HppcMaps.newNoNullKeysMap(settings.getGroups("transport.profiles.").size());
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
