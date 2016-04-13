/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.permission.Role;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.shield.support.Validation;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackPlugin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.shield.Security.setting;

/**
 *
 */
public class FileRolesStore extends AbstractLifecycleComponent<RolesStore> implements RolesStore {

    public static final Setting<String> ROLES_FILE_SETTING =
            Setting.simpleString(setting("authz.store.files.roles"), Property.NodeScope);
    private static final Pattern IN_SEGMENT_LINE = Pattern.compile("^\\s+.+");
    private static final Pattern SKIP_LINE = Pattern.compile("(^#.*|^\\s*)");

    private final Path file;
    private final RefreshListener listener;
    private final ResourceWatcherService watcherService;

    private volatile Map<String, Role> permissions;

    @Inject
    public FileRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService) {
        this(settings, env, watcherService, RefreshListener.NOOP);
    }

    public FileRolesStore(Settings settings, Environment env, ResourceWatcherService watcherService, RefreshListener listener) {
        super(settings);
        this.file = resolveFile(settings, env);
        this.listener = listener;
        this.watcherService = watcherService;
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
        permissions = parseFile(file, logger, settings);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public Role role(String role) {
        return permissions.get(role);
    }

    public static Path resolveFile(Settings settings, Environment env) {
        String location = ROLES_FILE_SETTING.get(settings);
        if (location.isEmpty()) {
            return XPackPlugin.resolveConfigFile(env, "roles.yml");
        }

        return XPackPlugin.resolveConfigFile(env, location);
    }

    public static Set<String> parseFileForRoleNames(Path path, ESLogger logger) {
        Map<String, Role> roleMap = parseFile(path, logger, false, Settings.EMPTY);
        if (roleMap == null) {
            return emptySet();
        }
        return roleMap.keySet();
    }

    public static Map<String, Role> parseFile(Path path, ESLogger logger, Settings settings) {
        return parseFile(path, logger, true, settings);
    }

    public static Map<String, Role> parseFile(Path path, ESLogger logger, boolean resolvePermission, Settings settings) {
        if (logger == null) {
            logger = NoOpLogger.INSTANCE;
        }

        Map<String, Role> roles = new HashMap<>();
        logger.trace("attempted to read roles file located at [{}]", path.toAbsolutePath());
        if (Files.exists(path)) {
            try {
                List<String> roleSegments = roleSegments(path);
                for (String segment : roleSegments) {
                    Role role = parseRole(segment, path, logger, resolvePermission, settings);
                    if (role != null) {
                        if (ReservedRolesStore.isReserved(role.name())) {
                            logger.warn("role [{}] is reserved. the relevant role definition in the mapping file will be ignored",
                                    role.name());
                        } else {
                            roles.put(role.name(), role);
                        }
                    }
                }

            } catch (IOException ioe) {
                logger.error("failed to read roles file [{}]. skipping all roles...", ioe, path.toAbsolutePath());
            }
        }

        return unmodifiableMap(roles);
    }

    private static Role parseRole(String segment, Path path, ESLogger logger, boolean resolvePermissions, Settings settings) {
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
                        logger.error("invalid role definition [{}] in roles file [{}]. invalid role name - {}. skipping role... ",
                                roleName, path.toAbsolutePath(), validationError);
                        return null;
                    }

                    if (resolvePermissions == false) {
                        return Role.builder(roleName).build();
                    }

                    token = parser.nextToken();
                    if (token == XContentParser.Token.START_OBJECT) {
                        RoleDescriptor descriptor = RoleDescriptor.parse(roleName, parser);

                        // first check if FLS/DLS is enabled on the role...
                        for (RoleDescriptor.IndicesPrivileges privilege : descriptor.getIndicesPrivileges()) {
                            if ((privilege.getQuery() != null || privilege.getFields() != null)
                                    && Security.flsDlsEnabled(settings) == false) {
                                logger.error("invalid role definition [{}] in roles file [{}]. document and field level security is not " +
                                        "enabled. set [{}] to [true] in the configuration file. skipping role...", roleName, path
                                        .toAbsolutePath(), XPackPlugin.featureEnabledSetting(Security.DLS_FLS_FEATURE));
                                return null;
                            }
                        }

                        return Role.builder(descriptor).build();
                    } else {
                        logger.error("invalid role definition [{}] in roles file [{}]. skipping role...", roleName, path.toAbsolutePath());
                        return null;
                    }
                }
            }
            logger.error("invalid role definition [{}] in roles file [{}]. skipping role...", roleName, path.toAbsolutePath());
        } catch (ElasticsearchParseException e) {
            assert roleName != null;
            if (logger.isDebugEnabled()) {
                logger.debug("parsing exception for role [{}]", e, roleName);
            } else {
                logger.error(e.getMessage() + ". skipping role...");
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
                    permissions = parseFile(file, logger, settings);
                    logger.info("updated roles (roles file [{}] changed)", file.toAbsolutePath());
                } catch (Throwable t) {
                    logger.error("could not reload roles file [{}]. Current roles remain unmodified", t, file.toAbsolutePath());
                    return;
                }
                listener.onRefresh();
            }
        }
    }

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(ROLES_FILE_SETTING);
    }
}
