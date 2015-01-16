/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.shield.support.Validation;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 *
 */
public class FileRolesStore extends AbstractLifecycleComponent<RolesStore> implements RolesStore {

    private static final Pattern COMMA_DELIM = Pattern.compile("\\s*,\\s*");
    private static final Pattern IN_SEGMENT_LINE = Pattern.compile("^\\s+.+");
    private static final Pattern SKIP_LINE = Pattern.compile("(^#.*|^\\s*)");

    private final Path file;
    private final Listener listener;

    private volatile ImmutableMap<String, Permission.Global.Role> permissions;

    @Inject
    public FileRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService) {
        this(settings, env, watcherService, Listener.NOOP);
    }

    public FileRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService, Listener listener) {
        super(settings);
        this.file = resolveFile(settings, env);
        this.listener = listener;
        permissions = ImmutableMap.of();

        FileWatcher watcher = new FileWatcher(file.getParent().toFile());
        watcher.addListener(new FileListener());
        watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        permissions = parseFile(file, logger);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public Permission.Global.Role role(String role) {
        return permissions.get(role);
    }

    public static Path resolveFile(Settings settings, Environment env) {
        String location = settings.get("shield.authz.store.files.roles");
        if (location == null) {
            return ShieldPlugin.resolveConfigFile(env, "roles.yml");
        }

        return Paths.get(location);
    }

    public static ImmutableMap<String, Permission.Global.Role> parseFile(Path path, ESLogger logger) {
        if (logger == null) {
            logger = NoOpLogger.INSTANCE;
        }

        logger.trace("reading roles file located at [{}]", path.toAbsolutePath());

        if (!Files.exists(path)) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, Permission.Global.Role> roles = ImmutableMap.builder();

        try {

            List<String> roleSegments = roleSegments(path);
            for (String segment : roleSegments) {
                Permission.Global.Role role = parseRole(segment, path, logger);
                if (role != null) {
                    roles.put(role.name(), role);
                }
            }

        } catch (IOException ioe) {
            logger.error("failed to read roles file [{}]. skipping all roles...", ioe, path.toAbsolutePath());
        }
        return roles.build();
    }

    private static Permission.Global.Role parseRole(String segment, Path path, ESLogger logger) {
        String roleName = null;
        try {
            XContentParser parser = YamlXContent.yamlXContent.createParser(segment);
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                    roleName = parser.currentName();
                    Validation.Error validationError = Validation.Roles.validateRoleName(roleName);
                    if (validationError != null) {
                        logger.error("invalid role definition [{}] in roles file [{}]. invalid role name - {}. skipping role... ", roleName, path.toAbsolutePath(), validationError);
                        return null;
                    }
                    Permission.Global.Role.Builder permission = Permission.Global.Role.builder(roleName);
                    token = parser.nextToken();
                    if (token == XContentParser.Token.START_OBJECT) {
                        String currentFieldName = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if ("cluster".equals(currentFieldName)) {
                                Privilege.Name name = null;
                                if (token == XContentParser.Token.VALUE_STRING) {
                                    String namesStr = parser.text().trim();
                                    if (Strings.hasLength(namesStr)) {
                                        String[] names = COMMA_DELIM.split(namesStr);
                                        name = new Privilege.Name(names);
                                    }
                                } else if (token == XContentParser.Token.START_ARRAY) {
                                    Set<String> names = new HashSet<>();
                                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                        if (token == XContentParser.Token.VALUE_STRING) {
                                            names.add(parser.text());
                                        }
                                    }
                                    if (!names.isEmpty()) {
                                        name = new Privilege.Name(names);
                                    }
                                } else {
                                    logger.error("invalid role definition [{}] in roles file [{}]. [cluster] field value can either " +
                                                    "be a string or a list of strings, but [{}] was found instead. skipping role...",
                                            roleName, path.toAbsolutePath(), token);
                                    return null;
                                }
                                if (name != null) {
                                    try {
                                        permission.set(Privilege.Cluster.get(name));
                                    } catch (ElasticsearchIllegalArgumentException e) {
                                        logger.error("invalid role definition [{}] in roles file [{}]. could not resolve cluster privileges [{}]. skipping role...", roleName, path.toAbsolutePath(), name);
                                        return null;
                                    }
                                }
                            } else if ("indices".equals(currentFieldName)) {
                                if (token == XContentParser.Token.START_OBJECT) {
                                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                        if (token == XContentParser.Token.FIELD_NAME) {
                                            currentFieldName = parser.currentName();
                                        } else if (Strings.hasLength(currentFieldName)) {
                                            String[] indices = COMMA_DELIM.split(currentFieldName);
                                            Privilege.Name name = null;
                                            if (token == XContentParser.Token.VALUE_STRING) {
                                                String namesStr = parser.text().trim();
                                                if (Strings.hasLength(namesStr)) {
                                                    String[] names = COMMA_DELIM.split(parser.text());
                                                    name = new Privilege.Name(names);
                                                }
                                            } else if (token == XContentParser.Token.START_ARRAY) {
                                                Set<String> names = new HashSet<>();
                                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                                    if (token == XContentParser.Token.VALUE_STRING) {
                                                        names.add(parser.text());
                                                    } else {
                                                        logger.error("invalid role definition [{}] in roles file [{}]. could not parse " +
                                                                "[{}] as index privilege. privilege names must be strings. skipping role...", roleName, path.toAbsolutePath(), token);
                                                        return null;
                                                    }
                                                }
                                                if (!names.isEmpty()) {
                                                    name = new Privilege.Name(names);
                                                }
                                            } else {
                                                logger.error("invalid role definition [{}] in roles file [{}]. could not parse [{}] as index privileges. privilege lists must either " +
                                                        "be a comma delimited string or an array of strings. skipping role...", roleName, path.toAbsolutePath(), token);
                                                return null;
                                            }
                                            if (name != null) {
                                                try {
                                                    permission.add(Privilege.Index.get(name), indices);
                                                } catch (ElasticsearchIllegalArgumentException e) {
                                                    logger.error("invalid role definition [{}] in roles file [{}]. could not resolve indices privileges [{}]. skipping role...", roleName, path.toAbsolutePath(), name);
                                                    return null;
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    logger.error("invalid role definition [{}] in roles file [{}]. [indices] field value must be an array of indices-privileges mappings defined as a string" +
                                                    " in the form <comma-separated list of index name patterns>::<comma-separated list of privileges> , but [{}] was found instead. skipping role...",
                                            roleName, path.toAbsolutePath(), token);
                                    return null;
                                }
                            }
                        }
                        return permission.build();
                    }
                    logger.error("invalid role definition [{}] in roles file [{}]. skipping role...", roleName, path.toAbsolutePath());
                }
            }
        } catch (YAMLException | IOException e) {
            if (roleName != null) {
                logger.error("invalid role definition [{}] in roles file [{}]. skipping role...", e, roleName, path);
            } else {
                logger.error("invalid role definition in roles file [{}]. skipping role...", e, path);
            }
        }
        return null;
    }

    private static List<String> roleSegments(Path path) throws IOException {
        List<String> segments = new ArrayList<>();
        StringBuilder builder = null;
        for (String line : Files.readAllLines(path, Charsets.UTF_8)) {
            if (!SKIP_LINE.matcher(line).matches()) {
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

    static interface Listener {

        static final Listener NOOP = new Listener() {
            @Override
            public void onRefresh() {
            }
        };

        void onRefresh();
    }

    private class FileListener extends FileChangesListener {

        @Override
        public void onFileCreated(File file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(File file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(File file) {
            if (file.equals(FileRolesStore.this.file.toFile())) {
                try {
                    permissions = parseFile(file.toPath(), logger);
                    logger.info("updated roles (roles file [{}] changed)", file.getAbsolutePath());
                } catch (Throwable t) {
                    logger.error("could not reload roles file [{}]. Current roles remain unmodified", t, file.getAbsolutePath());
                    return;
                }
                listener.onRefresh();
            }
        }
    }
}
