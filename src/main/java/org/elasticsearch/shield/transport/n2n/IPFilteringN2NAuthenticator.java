/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.n2n;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.net.InetAddresses;
import org.elasticsearch.common.netty.handler.ipfilter.IpFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.IpSubnetFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.PatternRule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.plugin.SecurityPlugin;
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
import java.util.regex.Pattern;

public class IPFilteringN2NAuthenticator extends AbstractComponent implements N2NAuthenticator {

    private static final Pattern COMMA_DELIM = Pattern.compile("\\s*,\\s*");
    private static final String DEFAULT_FILE = ".ip_filter.yml";
    private static final IpFilterRule[] NO_RULES = new IpFilterRule[0];

    private final Path file;

    private volatile IpFilterRule[] rules = NO_RULES;

    @Inject
    public IPFilteringN2NAuthenticator(Settings settings, Environment env, ResourceWatcherService watcherService) {
        super(settings);
        file = resolveFile(componentSettings, env);
        rules = parseFile(file, logger);
        FileWatcher watcher = new FileWatcher(file.getParent().toFile());
        watcher.addListener(new FileListener());
        watcherService.add(watcher);
    }

    private Path resolveFile(Settings settings, Environment env) {
        String location = settings.get("ip_filter.file");
        if (location == null) {
            File shieldDirectory = new File(env.configFile(), SecurityPlugin.NAME);
            return shieldDirectory.toPath().resolve(DEFAULT_FILE);
        }
        return Paths.get(location);
    }

    public static IpFilterRule[] parseFile(Path path, ESLogger logger) {
        if (!Files.exists(path)) {
            logger.info("No IP filtering rules loaded, as file {} does not exist. Rejecting all incoming connections!", path);
            return NO_RULES;
        }

        List<IpFilterRule> rules = new ArrayList<>();

        try (XContentParser parser = YamlXContent.yamlXContent.createParser(Files.newInputStream(path))) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT && token != null) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    if (!"allow".equals(currentFieldName) && !"deny".equals(currentFieldName)) {
                        throw new ElasticsearchParseException("Field name [" + currentFieldName + "] not valid. Must be [allow] or [deny]");
                    }
                } else if (token == XContentParser.Token.VALUE_STRING && currentFieldName != null) {
                    String value = parser.text();
                    if (!Strings.hasLength(value)) {
                        throw new ElasticsearchParseException("Field value for fieldname [" + currentFieldName + "] must not be empty");
                    }

                    boolean isAllowRule = currentFieldName.equals("allow");

                    if (value.contains(",")) {
                        for (String rule : COMMA_DELIM.split(parser.text().trim())) {
                            rules.add(getRule(isAllowRule, rule));
                        }
                    } else {
                        rules.add(getRule(isAllowRule, value));
                    }

                }
            }
        } catch (IOException | YAMLException e) {
            throw new ElasticsearchParseException("Failed to read & parse host access file [" + path.toAbsolutePath() + "]", e);
        }

        if (rules.size() == 0) {
            logger.info("No IP filtering rules loaded. Rejecting all incoming connections!");
            return NO_RULES;
        }

        logger.debug("Loaded {} ip filtering rules", rules.size());
        return rules.toArray(new IpFilterRule[rules.size()]);
    }

    private static IpFilterRule getRule(boolean isAllowRule, String value) throws UnknownHostException {
        if ("all".equals(value)) {
            return new PatternRule(isAllowRule, "n:*");
        } else if (value.contains("/")) {
            return new IpSubnetFilterRule(isAllowRule, value);
        }

        boolean isInetAddress = InetAddresses.isInetAddress(value);
        String prefix = isInetAddress ? "i:" : "n:";
        return new PatternRule(isAllowRule, prefix + value);
    }

    @Override
    public boolean authenticate(@Nullable Principal peerPrincipal, InetAddress peerAddress, int peerPort) {
        for (IpFilterRule rule : rules) {
            if (rule.contains(peerAddress)) {
                boolean isAllowed =  rule.isAllowRule();
                logger.trace("Authentication rule matched for host [{}]: {}", peerAddress, isAllowed);
                return isAllowed;
            }
        }

        logger.trace("Rejecting host {}", peerAddress);
        return false;
    }

    private class FileListener extends FileChangesListener {
        @Override
        public void onFileCreated(File file) {
            if (file.equals(IPFilteringN2NAuthenticator.this.file.toFile())) {
                rules = parseFile(file.toPath(), logger);
            }
        }

        @Override
        public void onFileDeleted(File file) {
            if (file.equals(IPFilteringN2NAuthenticator.this.file.toFile())) {
                rules = NO_RULES;
            }
        }

        @Override
        public void onFileChanged(File file) {
            if (file.equals(IPFilteringN2NAuthenticator.this.file.toFile())) {
                rules = parseFile(file.toPath(), logger);
            }
        }
    }
}
