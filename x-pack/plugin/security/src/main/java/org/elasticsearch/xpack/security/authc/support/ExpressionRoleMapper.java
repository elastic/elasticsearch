/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Strings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.core.Strings.format;

/**
 * This class loads and monitors the file defining the mappings of DNs to internal ES Roles.
 */
public class ExpressionRoleMapper implements UserRoleMapper {

    private static final Pattern IN_SEGMENT_LINE = Pattern.compile("^\\s+.+");
    private static final Pattern SKIP_LINE = Pattern.compile("(^#.*|^\\s*)");
    private static final Logger logger = LogManager.getLogger(ExpressionRoleMapper.class);

    private final NamedXContentRegistry xContentRegistry;
    private final Path file;
    private final List<ExpressionRoleMapping> expressionRoleMappings;
    private final ScriptService scriptService;

    public ExpressionRoleMapper(RealmConfig config, NamedXContentRegistry xContentRegistry, ScriptService scriptService) {
        this.xContentRegistry = xContentRegistry;
        this.scriptService = scriptService;
        this.file = resolveFile(config);
        this.expressionRoleMappings = parseFileLenient(file, logger, config.type(), config.name());
    }

    public static Path resolveFile(RealmConfig realmConfig) {
        String location = realmConfig.getSetting(DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING);
        return XPackPlugin.resolveConfigFile(realmConfig.env(), location);
    }

    public List<ExpressionRoleMapping> parseFileLenient(Path path, Logger logger, String realmType, String realmName) {
        try {
            return parseFile(path, logger, realmType, realmName, false);
        } catch (Exception e) {
            logger.error(
                () -> format("failed to parse role mappings file [%s]. skipping/removing all mappings...", path.toAbsolutePath()),
                e
            );
            return emptyList();
        }
    }

    public List<ExpressionRoleMapping> parseFile(Path path, Logger logger, String realmType, String realmName, boolean strict) {
        logger.trace("reading realm [{}/{}] role mappings file [{}]...", realmType, realmName, path.toAbsolutePath());

        if (Files.exists(path) == false) {
            final String message = Strings.format(
                "Role mapping file [%s] for realm [%s] does not exist.",
                path.toAbsolutePath(),
                realmName
            );
            if (strict) {
                throw new ElasticsearchException(message);
            } else {
                logger.warn(message + " Role mapping will be skipped.");
                return emptyList();
            }
        }

        try {
            return parseExpressionRoleMappings(file, xContentRegistry);
        } catch (SettingsException | IOException e) {
            throw new ElasticsearchException(
                "could not read realm [" + realmType + "/" + realmName + "] role mappings file [" + path.toAbsolutePath() + "]",
                e
            );
        }
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        try {
            final ExpressionModel model = user.asModel();
            final Set<String> roles = expressionRoleMappings.stream()
                .filter(ExpressionRoleMapping::isEnabled)
                .filter(m -> m.getExpression().match(model))
                .flatMap(m -> {
                    final Set<String> roleNames = m.getRoleNames(scriptService, model);
                    logger.trace("Applying role-mapping [{}] to user-model [{}] produced role-names [{}]", m.getName(), model, roleNames);
                    return roleNames.stream();
                })
                .collect(Collectors.toSet());
            logger.debug("Mapping user [{}] to roles [{}]", user, roles);
            listener.onResponse(roles);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void refreshRealmOnChange(CachingRealm realm) {

    }

    public static List<ExpressionRoleMapping> parseExpressionRoleMappings(Path path, NamedXContentRegistry xContentRegistry)
        throws IOException {
        return segments(path).stream().map(segment -> {
            final XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
                .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
            try {
                XContentParser parser = YamlXContent.yamlXContent.createParser(parserConfig, segment);
                XContentParser.Token token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    token = parser.nextToken();
                    if (token == XContentParser.Token.FIELD_NAME) {
                        var mappingName = parser.currentName();
                        token = parser.nextToken();
                        if (token == XContentParser.Token.START_OBJECT) {
                            return ExpressionRoleMapping.parse(mappingName, parser);
                        } else {
                            logger.error(
                                "invalid role mapping definition [{}] in role mapping file [{}]. skipping role mapping...",
                                mappingName,
                                path.toAbsolutePath()
                            );
                            return null;
                        }
                    }
                }
            } catch (ElasticsearchParseException | IOException | XContentParseException e) {
                logger.error("bad role mapping definition", e);
            }
            return null;
        }).collect(Collectors.toList());
    }

    private static List<String> segments(Path path) throws IOException {
        final List<String> segments = new ArrayList<>();
        StringBuilder builder = null;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (SKIP_LINE.matcher(line).matches() == false) {
                if (IN_SEGMENT_LINE.matcher(line).matches()) {
                    if (builder != null) {
                        builder.append(line).append("\n");
                    }
                } else {
                    if (builder != null) {
                        segments.add(builder.toString());
                    }
                    builder = new StringBuilder(line).append("\n");
                }
            }
        }
        if (builder != null) {
            segments.add(builder.toString());
        }
        return segments;
    }

}
