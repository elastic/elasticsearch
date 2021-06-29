/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.filter;

import io.netty.handler.ipfilter.IpFilterRule;
import io.netty.handler.ipfilter.IpFilterRuleType;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.network.NetworkAddress;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.regex.Pattern;

/**
 * The Class PatternRule represents an IP filter rule using string patterns.
 * <br>
 * Rule Syntax:
 * <br>
 * <pre>
 * Rule ::= [n|i]:address          n stands for computer name, i for ip address
 * address ::= &lt;regex&gt; | localhost
 * regex is a regular expression with '*' as multi character and '?' as single character wild card
 * </pre>
 * <br>
 * Example: allow localhost:
 * <br>
 * new PatternRule(true, "n:localhost")
 * <br>
 * Example: allow local lan:
 * <br>
 * new PatternRule(true, "i:192.168.0.*")
 * <br>
 * Example: block all
 * <br>
 * new PatternRule(false, "n:*")
 * <br>
 */
// this has been adopted from Netty3 there is no replacement in netty4 for this.
final class PatternRule implements IpFilterRule {

    private final Pattern ipPattern;
    private final Pattern namePattern;
    private final IpFilterRuleType ruleType;
    private final boolean localhost;
    private final String pattern;

    /**
     * Instantiates a new pattern rule.
     *
     * @param ruleType   indicates if this is an allow or block rule
     * @param pattern the filter pattern
     */
    PatternRule(IpFilterRuleType ruleType, String pattern) {
        this.ruleType = ruleType;
        this.pattern = pattern;
        Pattern namePattern = null;
        Pattern ipPattern = null;
        boolean localhost = false;
        if (pattern != null) {
            String[] acls = pattern.split(",");
            String ip = "";
            String name = "";
            for (String c : acls) {
                c = c.trim();
                if ("n:localhost".equals(c)) {
                    localhost = true;
                } else if (c.startsWith("n:")) {
                    name = addRule(name, c.substring(2));
                } else if (c.startsWith("i:")) {
                    ip = addRule(ip, c.substring(2));
                }
            }
            if (ip.length() != 0) {
                ipPattern = Pattern.compile(ip);
            }
            if (name.length() != 0) {
                namePattern = Pattern.compile(name);
            }
        }
        this.ipPattern = ipPattern;
        this.namePattern = namePattern;
        this.localhost = localhost;
    }

    /**
     * returns the pattern.
     *
     * @return the pattern
     */
    String getPattern() {
        return pattern;
    }

    private static String addRule(String pattern, String rule) {
        if (rule == null || rule.length() == 0) {
            return pattern;
        }
        if (pattern.length() != 0) {
            pattern += "|";
        }
        rule = rule.replaceAll("\\.", "\\\\.");
        rule = rule.replaceAll("\\*", ".*");
        rule = rule.replaceAll("\\?", ".");
        pattern += '(' + rule + ')';
        return pattern;
    }

    private boolean isLocalhost(InetAddress address) {
        try {
            return address.isAnyLocalAddress() || address.isLoopbackAddress() || NetworkInterface.getByInetAddress(address) != null;
        } catch (SocketException e) {
            // not defined - ie. it's not a local address
            return false;
        }
    }


    @Override
    public boolean matches(InetSocketAddress remoteAddress) {
        InetAddress inetAddress = remoteAddress.getAddress();
        if (localhost) {
            if (isLocalhost(inetAddress)) {
                return true;
            }
        }
        if (ipPattern != null) {
            String format = NetworkAddress.format(inetAddress);
            if (ipPattern.matcher(format).matches()) {
                return true;
            }
        }

        return checkHostName(inetAddress);
    }

    @SuppressForbidden(reason = "we compare the hostname of the address this is how netty3 did it and we keep it for BWC")
    private boolean checkHostName(InetAddress address) {
        if (namePattern != null) {
            if (namePattern.matcher(address.getHostName()).matches()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public IpFilterRuleType ruleType() {
        return ruleType;
    }

    boolean isLocalhost() {
        return localhost;
    }
}
