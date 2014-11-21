/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.n2n;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.common.net.InetAddresses;
import org.elasticsearch.common.netty.handler.ipfilter.IpFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.IpSubnetFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.PatternRule;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class IPFilteringN2NAuthenticator extends AbstractComponent implements N2NAuthenticator {

    private static final Pattern COMMA_DELIM = Pattern.compile("\\s*,\\s*");
    private static final ProfileIpFilterRule[] NO_RULES = new ProfileIpFilterRule[0];
    private volatile ProfileIpFilterRule[] rules = NO_RULES;

    @Inject
    public IPFilteringN2NAuthenticator(Settings settings) {
        super(settings);
        parseSettings();
    }

    private void parseSettings() {
        String[] allowed = settings.getAsArray("shield.transport.filter.allow");
        String[] denied = settings.getAsArray("shield.transport.filter.deny");
        List<ProfileIpFilterRule> rules = new ArrayList<>();

        try {
            rules.addAll(parseValue(allowed, "default", true));
            rules.addAll(parseValue(denied, "default", false));

            Map<String, Settings> groupedSettings = settings.getGroups("transport.profiles.");
            for (Map.Entry<String, Settings> entry : groupedSettings.entrySet()) {
                String profile = entry.getKey();
                Settings settings = entry.getValue().getByPrefix("shield.filter.");
                rules.addAll(parseValue(settings.getAsArray("allow"), profile, true));
                rules.addAll(parseValue(settings.getAsArray("deny"), profile, false));
            }

        } catch (IOException | YAMLException e) {
            throw new ElasticsearchParseException("Failed to read & parse rules from settings", e);
        }

        logger.debug("Loaded {} ip filtering rules", rules.size());
        this.rules = rules.toArray(new ProfileIpFilterRule[rules.size()]);
    }

    private Collection<? extends ProfileIpFilterRule> parseValue(String[] values, String profile, boolean isAllowRule) throws UnknownHostException {
        List<ProfileIpFilterRule> rules = new ArrayList<>();
        for (String value : values) {
            rules.add(new ProfileIpFilterRule(profile, getRule(isAllowRule, value)));
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

    @Override
    public boolean authenticate(@Nullable Principal peerPrincipal, String profile, InetAddress peerAddress, int peerPort) {
        for (ProfileIpFilterRule rule : rules) {
            if (rule.contains(profile, peerAddress)) {
                boolean isAllowed = rule.isAllowRule();
                logger.trace("Authentication rule matched for host [{}]: {}", peerAddress, isAllowed);
                return isAllowed;
            }
        }

        logger.trace("Allowing host {}", peerAddress);
        return true;
    }
}
