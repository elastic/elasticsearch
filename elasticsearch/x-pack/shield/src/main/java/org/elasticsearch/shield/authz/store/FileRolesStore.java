/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.shield.authz.SystemRole;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.shield.support.Validation;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 *
 */
public class FileRolesStore extends AbstractLifecycleComponent<RolesStore> implements RolesStore {

    private static final Pattern COMMA_DELIM = Pattern.compile("\\s*,\\s*");
    private static final Pattern IN_SEGMENT_LINE = Pattern.compile("^\\s+.+");
    private static final Pattern SKIP_LINE = Pattern.compile("(^#.*|^\\s*)");

    private final Path file;
    private final RefreshListener listener;
    private final Set<Permission.Global.Role> reservedRoles;
    private final ResourceWatcherService watcherService;

    private volatile Map<String, Permission.Global.Role> permissions;

    @Inject
    public FileRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService, Set<Permission.Global.Role> reservedRoles) {
        this(settings, env, watcherService, reservedRoles, RefreshListener.NOOP);
    }

    public FileRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService, Set<Permission.Global.Role> reservedRoles, RefreshListener listener) {
        super(settings);
        this.file = resolveFile(settings, env);
        this.listener = listener;
        this.watcherService = watcherService;
        this.reservedRoles = unmodifiableSet(new HashSet<>(reservedRoles));
        permissions = emptyMap();
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        FileWatcher watcher = new FileWatcher(file.getParent());
        watcher.addListener(new FileListener());
        try {
            watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to setup roles file watcher", e);
        }
        permissions = parseFile(file, reservedRoles, logger, settings);
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

        return env.binFile().getParent().resolve(location);
    }

    public static Set<String> parseFileForRoleNames(Path path, ESLogger logger) {
        Map<String, Permission.Global.Role> roleMap = parseFile(path, Collections.<Permission.Global.Role>emptySet(), logger, false, Settings.EMPTY);
        if (roleMap == null) {
            return emptySet();
        }
        return roleMap.keySet();
    }

    public static Map<String, Permission.Global.Role> parseFile(Path path, Set<Permission.Global.Role> reservedRoles, ESLogger logger, Settings settings) {
        return parseFile(path, reservedRoles, logger, true, settings);
    }

    public static Map<String, Permission.Global.Role> parseFile(Path path, Set<Permission.Global.Role> reservedRoles, ESLogger logger, boolean resolvePermission, Settings settings) {
        if (logger == null) {
            logger = NoOpLogger.INSTANCE;
        }

        Map<String, Permission.Global.Role> roles = new HashMap<>();
        logger.trace("attempted to read roles file located at [{}]", path.toAbsolutePath());
        if (Files.exists(path)) {
            try {
                List<String> roleSegments = roleSegments(path);
                for (String segment : roleSegments) {
                    Permission.Global.Role role = parseRole(segment, path, logger, resolvePermission, settings);
                    if (role != null) {
                        if (SystemRole.NAME.equals(role.name())) {
                            logger.warn("role [{}] is reserved to the system. the relevant role definition in the mapping file will be ignored", SystemRole.NAME);
                        } else {
                            roles.put(role.name(), role);
                        }
                    }
                }

            } catch (IOException ioe) {
                logger.error("failed to read roles file [{}]. skipping all roles...", ioe, path.toAbsolutePath());
            }
        }

        // we now add all the fixed roles (overriding any attempts to override the fixed roles in the file)
        for (Permission.Global.Role reservedRole : reservedRoles) {
            if (roles.containsKey(reservedRole.name())) {
                logger.warn("role [{}] is reserved to the system. the relevant role definition in the mapping file will be ignored", reservedRole.name());
            }
            roles.put(reservedRole.name(), reservedRole);
        }

        return unmodifiableMap(roles);
    }

    private static Permission.Global.Role parseRole(String segment, Path path, ESLogger logger, boolean resolvePermissions, Settings settings) {
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
                    if (resolvePermissions == false) {
                        return permission.build();
                    }

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
                                        permission.cluster(Privilege.Cluster.get(name));
                                    } catch (IllegalArgumentException e) {
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
                                            } else if (token == XContentParser.Token.START_OBJECT) {
                                                List<String> fields = null;
                                                BytesReference query = null;
                                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                                    if (token == XContentParser.Token.FIELD_NAME) {
                                                        currentFieldName = parser.currentName();
                                                    } else if ("fields".equals(currentFieldName)) {
                                                        if (token == XContentParser.Token.START_ARRAY) {
                                                            fields = (List) parser.list();
                                                        } else if (token.isValue()) {
                                                            String field = parser.text();
                                                            if (field.trim().isEmpty()) {
                                                                // The yaml parser doesn't emit null token if the key is empty...
                                                                fields = Collections.emptyList();
                                                            } else {
                                                                fields = Collections.singletonList(field);
                                                            }
                                                        }
                                                    } else if ("query".equals(currentFieldName)) {
                                                        if (token == XContentParser.Token.START_OBJECT) {
                                                            XContentBuilder builder = JsonXContent.contentBuilder();
                                                            XContentHelper.copyCurrentStructure(builder.generator(), parser);
                                                            query = builder.bytes();
                                                        } else if (token == XContentParser.Token.VALUE_STRING) {
                                                            query = new BytesArray(parser.text());
                                                        }
                                                    } else if ("privileges".equals(currentFieldName)) {
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
                                                        }
                                                    }
                                                }
                                                if (name != null) {
                                                    if ((query != null || (fields != null && fields.isEmpty() == false)) && ShieldPlugin.flsDlsEnabled(settings) == false) {
                                                        logger.error("invalid role definition [{}] in roles file [{}]. document and field level security is not enabled. set [{}] to [true] in the configuration file. skipping role...", roleName, path.toAbsolutePath(), ShieldPlugin.DLS_FLS_ENABLED_SETTING);
                                                        return null;
                                                    }

                                                    try {
                                                        permission.add(fields, query, Privilege.Index.get(name), indices);
                                                    } catch (IllegalArgumentException e) {
                                                        logger.error("invalid role definition [{}] in roles file [{}]. could not resolve indices privileges [{}]. skipping role...", roleName, path.toAbsolutePath(), name);
                                                        return null;
                                                    }
                                                }
                                                continue;
                                            } else {
                                                logger.error("invalid role definition [{}] in roles file [{}]. could not parse [{}] as index privileges. privilege lists must either " +
                                                        "be a comma delimited string or an array of strings. skipping role...", roleName, path.toAbsolutePath(), token);
                                                return null;
                                            }
                                            if (name != null) {
                                                try {
                                                    permission.add(Privilege.Index.get(name), indices);
                                                } catch (IllegalArgumentException e) {
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
                            } else if ("run_as".equals(currentFieldName)) {
                                Set<String> names = new HashSet<>();
                                if (token == XContentParser.Token.VALUE_STRING) {
                                    String namesStr = parser.text().trim();
                                    if (Strings.hasLength(namesStr)) {
                                        String[] namesArr = COMMA_DELIM.split(namesStr);
                                        names.addAll(Arrays.asList(namesArr));
                                    }
                                } else if (token == XContentParser.Token.START_ARRAY) {
                                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                        if (token == XContentParser.Token.VALUE_STRING) {
                                            names.add(parser.text());
                                        }
                                    }
                                } else {
                                    logger.error("invalid role definition [{}] in roles file [{}]. [run_as] field value can either " +
                                                    "be a string or a list of strings, but [{}] was found instead. skipping role...",
                                            roleName, path.toAbsolutePath(), token);
                                    return null;
                                }
                                if (!names.isEmpty()) {
                                    Privilege.Name name = new Privilege.Name(names);
                                    try {
                                        permission.runAs(new Privilege.General(new Privilege.Name(names), names.toArray(new String[names.size()])));
                                    } catch (IllegalArgumentException e) {
                                        logger.error("invalid role definition [{}] in roles file [{}]. could not resolve run_as privileges [{}]. skipping role...", roleName, path.toAbsolutePath(), name);
                                        return null;
                                    }
                                }
                            } else {
                                logger.warn("unknown field [{}] found in role definition [{}] in roles file [{}]", currentFieldName, roleName, path.toAbsolutePath());
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
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
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

    private class FileListener extends FileChangesListener {

        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(Path file) {
            if (file.equals(FileRolesStore.this.file)) {
                try {
                    permissions = parseFile(file, reservedRoles, logger, settings);
                    logger.info("updated roles (roles file [{}] changed)", file.toAbsolutePath());
                } catch (Throwable t) {
                    logger.error("could not reload roles file [{}]. Current roles remain unmodified", t, file.toAbsolutePath());
                    return;
                }
                listener.onRefresh();
            }
        }
    }
}
