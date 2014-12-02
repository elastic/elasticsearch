/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.filter;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.net.InetAddresses;
import org.elasticsearch.common.netty.handler.ipfilter.IpFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.IpSubnetFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.PatternRule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.audit.AuditTrail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class IPFilter extends AbstractComponent {

    /**
     * .http has been chosen for handling HTTP filters, which are not part of the profiles
     * The profiles are only handled for the transport protocol, so we need an own kind of profile
     * for HTTP. This name starts withs a dot, because no profile name can ever start like that due to
     * how we handle settings
     */
    public static final String HTTP_PROFILE_NAME = ".http";

    private static final ProfileIpFilterRule[] NO_RULES = new ProfileIpFilterRule[0];
    private static final ProfileIpFilterRule ACCEPT_ALL_RULE = new ProfileIpFilterRule("default",
            new PatternRule(true, "n:*"), "DEFAULT_ACCEPT_ALL");

    private final AuditTrail auditTrail;

    private volatile ProfileIpFilterRule[] rules = NO_RULES;

    @Inject
    public IPFilter(Settings settings, AuditTrail auditTrail) {
        super(settings);
        this.auditTrail = auditTrail;
        rules = parseSettings(settings, logger);
    }

    public boolean accept(String profile, InetAddress peerAddress) {
        if (rules == NO_RULES) {
            return true;
        }

        for (ProfileIpFilterRule rule : rules) {
            if (rule.contains(profile, peerAddress)) {
                boolean isAllowed = rule.isAllowRule();
                if (isAllowed) {
                    auditTrail.connectionGranted(peerAddress, rule);
                } else {
                    auditTrail.connectionDenied(peerAddress, rule);
                }
                return isAllowed;
            }
        }

        auditTrail.connectionGranted(peerAddress, ACCEPT_ALL_RULE);
        return true;
    }

    private static ProfileIpFilterRule[] parseSettings(Settings settings, ESLogger logger) {
        if (!settings.getAsBoolean("shield.transport.filter.enabled", true)) {
            return NO_RULES;
        }
        String[] allowed = settings.getAsArray("shield.transport.filter.allow");
        String[] denied = settings.getAsArray("shield.transport.filter.deny");
        String[] httpAllowed = settings.getAsArray("shield.http.filter.allow", settings.getAsArray("transport.profiles.default.shield.filter.allow", settings.getAsArray("shield.transport.filter.allow")));
        String[] httpDdenied = settings.getAsArray("shield.http.filter.deny", settings.getAsArray("transport.profiles.default.shield.filter.deny", settings.getAsArray("shield.transport.filter.deny")));
        List<ProfileIpFilterRule> rules = new ArrayList<>();

        try {
            rules.addAll(parseValue(allowed, "default", true));
            rules.addAll(parseValue(denied, "default", false));
            rules.addAll(parseValue(httpAllowed, HTTP_PROFILE_NAME, true));
            rules.addAll(parseValue(httpDdenied, HTTP_PROFILE_NAME, false));

            Map<String, Settings> groupedSettings = settings.getGroups("transport.profiles.");
            for (Map.Entry<String, Settings> entry : groupedSettings.entrySet()) {
                String profile = entry.getKey();
                Settings profileSettings = entry.getValue().getByPrefix("shield.filter.");
                rules.addAll(parseValue(profileSettings.getAsArray("allow"), profile, true));
                rules.addAll(parseValue(profileSettings.getAsArray("deny"), profile, false));
            }

        } catch (IOException | YAMLException e) {
            throw new ElasticsearchParseException("Failed to read & parse rules from settings", e);
        }

        logger.debug("Loaded {} ip filtering rules", rules.size());
        return rules.toArray(new ProfileIpFilterRule[rules.size()]);
    }

    private static Collection<? extends ProfileIpFilterRule> parseValue(String[] values, String profile, boolean isAllowRule) throws UnknownHostException {
        List<ProfileIpFilterRule> rules = new ArrayList<>();
        for (String value : values) {
            rules.add(new ProfileIpFilterRule(profile, getRule(isAllowRule, value), value));
        }
        return rules;
    }

    private static IpFilterRule getRule(boolean isAllowRule, String value) throws UnknownHostException {
        if ("_all".equals(value)) {
            return new PatternRule(isAllowRule, "n:*");
        } else if (value.contains("/")) {
            return new IpSubnetFilterRule(isAllowRule, value);
        }

        boolean isInetAddress = InetAddresses.isInetAddress(value);
        String prefix = isInetAddress ? "i:" : "n:";
        return new PatternRule(isAllowRule, prefix + value);
    }
}
