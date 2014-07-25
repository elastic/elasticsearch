/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.n2n;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.netty.handler.ipfilter.IpFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.IpSubnetFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.PatternRule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

/**
 * A file based IP Filtering node to node authentication. IP filtering rules can be configured in
 * a monitored file (auto loads when changed). Each line in the file represents a filtering rule.
 * A rule is composed of an inclusion/exclusion sign and a matching rule:
 *
 *
 * <ul>
 *     <li><code>-PATTERN</code>: means any remote address that matches PATTERN will be denied (auth will fail)<li>
 *     <li><code>+PATTERN</code>: means any remote address that matches PATTERN will be allowed (auth will succeed)<li>
 * </ul>
 *
 * The following patterns are supported:
 *
 * <ul>
 *     <li><code>i:IP</code> - where IP is a specific node IP (regexp supported)</li>
 *     <li><code>c:MASK</code> - where MASK is a CIDR mask to match nodes IPs</li>
 *     <li><code>n:HOST</code> - where HOST is the hostname of the node (regexp supported)</li>
 * </ul>
 *
 * Examples:
 *
 * <ul>
 *     <li><code>-i:192.168.100.2</code></li>: deny access from node with the specified IP
 *     <li><code>+c:2001:db8::/48</code></li>: allow access from nodes with IPs that match the specified mask
 *     <li><code>-n:es-staging-.*</code></li>: deny access from any node in stanging env (matched on the hostname regexp)
 * </ul>
 *
 */
public class IPFilteringN2NAuthentricator extends AbstractComponent implements N2NAuthenticator {

    private static final String DEFAULT_FILE = ".ip_filtering";
    private static final IpFilterRule[] NO_RULES = new IpFilterRule[0];

    private final Path file;
    private final FileWatcher watcher;

    private volatile IpFilterRule[] rules;

    @Inject
    public IPFilteringN2NAuthentricator(Settings settings, Environment env, ResourceWatcherService watcherService) {
        super(settings);
        file = resolveFile(componentSettings, env);
        rules = parseFile(file, logger);
        watcher = new FileWatcher(file.getParent().toFile());
        watcher.addListener(new FileListener());
        watcherService.add(watcher);
    }

    public static Path resolveFile(Settings settings, Environment env) {
        String location = settings.get("file" + DEFAULT_FILE);
        if (location == null) {
            return env.configFile().toPath().resolve(DEFAULT_FILE);
        }
        return Paths.get(location);
    }

    public static IpFilterRule[] parseFile(Path path, @Nullable ESLogger logger) {
        if (!Files.exists(path)) {
            return null;
        }
        List<String> lines = null;
        try {
            lines = Files.readAllLines(path, Charsets.UTF_8);
        } catch (IOException ioe) {
            throw new ElasticsearchException("Failed to read hosts file [" + path.toAbsolutePath() + "]", ioe);
        }

        List<IpFilterRule> rules = new ArrayList<>(lines.size());
        for (String line : lines) {
            rules.add(parseRule(path, line, logger));
        }

        if (rules.size() == 0) {
            return NO_RULES;
        }

        return rules.toArray(new IpFilterRule[rules.size()]);
    }

    private static IpFilterRule parseRule(Path path, String rule, @Nullable ESLogger logger) {
        if (rule == null || rule.length() == 0) {
            return null;
        }
        if (!(rule.startsWith("+") || rule.startsWith("-"))) {
            return null;
        }

        boolean allow = rule.startsWith("+");

        if (rule.charAt(1) == 'n' || rule.charAt(1) == 'i') {
            return new PatternRule(allow, rule.substring(1));
        }

        if (rule.charAt(1) == 'c') {
            try {
                return new IpSubnetFilterRule(allow, rule.substring(3));
            } catch (UnknownHostException e) {
                if (logger != null && logger.isErrorEnabled()) {
                    logger.error("Skipping invalid ip filtering rule [" + rule + "] in hosts_allow file [" + path.toAbsolutePath() + "]", e);
                }
                return null;
            }
        }
        if (logger != null && logger.isErrorEnabled()) {
            logger.error("Skipping invalid ip filtering rule [" + rule + "] in hosts_allow file [" + path.toAbsolutePath() + "]. ':' can only appear once");
        }
        return null;
    }

    @Override
    public boolean authenticate(@Nullable Principal peerPrincipal, InetAddress peerAddress, int peerPort) {
        for (int i = 0; i < rules.length; i++) {
            if (rules[i].contains(peerAddress)) {
                return true;
            }
        }
        return false;
    }

    private class FileListener extends FileChangesListener {
        @Override
        public void onFileCreated(File file) {
            if (file.equals(IPFilteringN2NAuthentricator.this.file.toFile())) {
                rules = parseFile(file.toPath(), logger);
            }
        }

        @Override
        public void onFileDeleted(File file) {
            if (file.equals(IPFilteringN2NAuthentricator.this.file.toFile())) {
                rules = NO_RULES;
            }
        }

        @Override
        public void onFileChanged(File file) {
            if (file.equals(IPFilteringN2NAuthentricator.this.file.toFile())) {
                if (file.equals(IPFilteringN2NAuthentricator.this.file.toFile())) {
                    rules = parseFile(file.toPath(), logger);
                }
            }
        }
    }
}
