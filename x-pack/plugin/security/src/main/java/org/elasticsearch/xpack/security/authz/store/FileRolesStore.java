/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.core.security.support.NoOpLogger;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.PrivilegedFileWatcher;
import org.elasticsearch.xpack.security.authz.FileRoleValidator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;

public class FileRolesStore implements BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>> {

    private static final Pattern IN_SEGMENT_LINE = Pattern.compile("^\\s+.+");
    private static final Pattern SKIP_LINE = Pattern.compile("(^#.*|^\\s*)");
    private static final Logger logger = LogManager.getLogger(FileRolesStore.class);
    private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder()
        .allow2xFormat(true)
        .allowDescription(true)
        .build();

    private final Settings settings;
    private final Path file;
    private final FileRoleValidator roleValidator;
    private final XPackLicenseState licenseState;
    private final NamedXContentRegistry xContentRegistry;
    private final List<Consumer<Set<String>>> listeners = new ArrayList<>();

    private volatile Map<String, RoleDescriptor> permissions;

    public FileRolesStore(
        Settings settings,
        Environment env,
        ResourceWatcherService watcherService,
        XPackLicenseState licenseState,
        NamedXContentRegistry xContentRegistry,
        FileRoleValidator roleValidator
    ) throws IOException {
        this(settings, env, watcherService, null, roleValidator, licenseState, xContentRegistry);
    }

    FileRolesStore(
        Settings settings,
        Environment env,
        ResourceWatcherService watcherService,
        Consumer<Set<String>> listener,
        FileRoleValidator roleValidator,
        XPackLicenseState licenseState,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        this.settings = settings;
        this.file = resolveFile(env);
        this.roleValidator = roleValidator;
        if (listener != null) {
            listeners.add(listener);
        }
        this.licenseState = licenseState;
        this.xContentRegistry = xContentRegistry;
        FileWatcher watcher = new PrivilegedFileWatcher(file.getParent());
        watcher.addListener(new FileListener());
        watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        permissions = parseFile(file, logger, settings, licenseState, xContentRegistry, roleValidator);
    }

    @Override
    public void accept(Set<String> names, ActionListener<RoleRetrievalResult> listener) {
        listener.onResponse(RoleRetrievalResult.success(roleDescriptors(names)));
    }

    Set<RoleDescriptor> roleDescriptors(Set<String> roleNames) {
        final Map<String, RoleDescriptor> localPermissions = permissions;
        Set<RoleDescriptor> descriptors = new HashSet<>();
        roleNames.forEach((name) -> {
            RoleDescriptor descriptor = localPermissions.get(name);
            if (descriptor != null) {
                descriptors.add(descriptor);
            }
        });
        return descriptors;
    }

    public boolean exists(String name) {
        final Map<String, RoleDescriptor> localPermissions = permissions;
        return localPermissions.containsKey(name);
    }

    public Map<String, Object> usageStats() {
        final Map<String, RoleDescriptor> localPermissions = permissions;
        Map<String, Object> usageStats = Maps.newMapWithExpectedSize(3);
        usageStats.put("size", localPermissions.size());

        boolean dls = false;
        boolean fls = false;
        for (RoleDescriptor descriptor : localPermissions.values()) {
            for (IndicesPrivileges indicesPrivileges : descriptor.getIndicesPrivileges()) {
                fls = fls || indicesPrivileges.getGrantedFields() != null || indicesPrivileges.getDeniedFields() != null;
                dls = dls || indicesPrivileges.getQuery() != null;
            }
            if (fls && dls) {
                break;
            }
        }

        usageStats.put("fls", fls);
        usageStats.put("dls", dls);
        usageStats.put("remote_indices", localPermissions.values().stream().filter(RoleDescriptor::hasRemoteIndicesPrivileges).count());
        usageStats.put("remote_cluster", localPermissions.values().stream().filter(RoleDescriptor::hasRemoteClusterPermissions).count());

        return usageStats;
    }

    public void addListener(Consumer<Set<String>> consumer) {
        Objects.requireNonNull(consumer);
        synchronized (this) {
            listeners.add(consumer);
        }
    }

    public Path getFile() {
        return file;
    }

    /**
     * @return a map of all file role definitions. The returned map is unmodifiable.
     */
    public Map<String, RoleDescriptor> getAllRoleDescriptors() {
        final Map<String, RoleDescriptor> localPermissions = permissions;
        return Collections.unmodifiableMap(localPermissions);
    }

    // package private for testing
    Set<String> getAllRoleNames() {
        return permissions.keySet();
    }

    @Override
    public String toString() {
        return "file roles store (" + file + ")";
    }

    public static Path resolveFile(Environment env) {
        return XPackPlugin.resolveConfigFile(env, "roles.yml");
    }

    public static Set<String> parseFileForRoleNames(Path path, Logger logger) {
        if (logger == null) {
            logger = NoOpLogger.INSTANCE;
        }

        Map<String, RoleDescriptor> roles = new HashMap<>();
        logger.trace("attempting to read roles file located at [{}]", path.toAbsolutePath());
        if (Files.exists(path)) {
            try {
                List<String> roleSegments = roleSegments(path);
                for (String segment : roleSegments) {
                    RoleDescriptor rd = parseRoleDescriptor(
                        segment,
                        path,
                        logger,
                        false,
                        Settings.EMPTY,
                        NamedXContentRegistry.EMPTY,
                        new FileRoleValidator.Default()
                    );
                    if (rd != null) {
                        roles.put(rd.getName(), rd);
                    }
                }
            } catch (IOException ioe) {
                logger.error(() -> format("failed to read roles file [%s]. skipping all roles...", path.toAbsolutePath()), ioe);
                return emptySet();
            }
        }
        return unmodifiableSet(roles.keySet());

    }

    public static Map<String, RoleDescriptor> parseFile(
        Path path,
        Logger logger,
        Settings settings,
        XPackLicenseState licenseState,
        NamedXContentRegistry xContentRegistry,
        FileRoleValidator roleValidator
    ) {
        if (logger == null) {
            logger = NoOpLogger.INSTANCE;
        }

        Map<String, RoleDescriptor> roles = new HashMap<>();
        logger.debug("attempting to read roles file located at [{}]", path.toAbsolutePath());
        if (Files.exists(path)) {
            try {
                List<String> roleSegments = roleSegments(path);

                final boolean isDlsLicensed = DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState);
                for (String segment : roleSegments) {
                    RoleDescriptor descriptor = parseRoleDescriptor(segment, path, logger, true, settings, xContentRegistry, roleValidator);
                    if (descriptor != null) {
                        if (ReservedRolesStore.isReserved(descriptor.getName())) {
                            logger.warn(
                                "role [{}] is reserved. the relevant role definition in the mapping file will be ignored",
                                descriptor.getName()
                            );
                        } else if (descriptor.isUsingDocumentOrFieldLevelSecurity() && isDlsLicensed == false) {
                            logger.warn(
                                "role [{}] uses document and/or field level security, which is not enabled by the current license"
                                    + ". this role will be ignored",
                                descriptor.getName()
                            );
                            // we still put the role in the map to avoid unnecessary negative lookups
                            roles.put(descriptor.getName(), descriptor);
                        } else {
                            roles.put(descriptor.getName(), descriptor);
                        }
                    }
                }
            } catch (IOException ioe) {
                logger.error(() -> format("failed to read roles file [%s]. skipping all roles...", path.toAbsolutePath()), ioe);
                return emptyMap();
            }
        } else {
            logger.debug("roles file does not exist");
            return emptyMap();
        }

        logger.info("parsed [{}] roles from file [{}]", roles.size(), path.toAbsolutePath());
        return unmodifiableMap(roles);
    }

    @Nullable
    static RoleDescriptor parseRoleDescriptor(
        String segment,
        Path path,
        Logger logger,
        boolean resolvePermissions,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        FileRoleValidator roleValidator
    ) {
        String roleName = null;
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser parser = YamlXContent.yamlXContent.createParser(parserConfig, segment)) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                    roleName = parser.currentName();
                    Validation.Error validationError = Validation.Roles.validateRoleName(roleName, false);
                    if (validationError != null) {
                        logger.error(
                            "invalid role definition [{}] in roles file [{}]. invalid role name - {}. skipping role...",
                            roleName,
                            path.toAbsolutePath(),
                            validationError
                        );
                        return null;
                    }

                    if (resolvePermissions == false) {
                        return new RoleDescriptor(roleName, null, null, null);
                    }

                    token = parser.nextToken();
                    if (token == XContentParser.Token.START_OBJECT) {
                        // we do not want to reject files if field permissions are given in 2.x syntax, hence why we allow2xFormat
                        RoleDescriptor descriptor = ROLE_DESCRIPTOR_PARSER.parse(roleName, parser);
                        return checkDescriptor(descriptor, path, logger, settings, xContentRegistry, roleValidator);
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
                final String finalRoleName = roleName;
                logger.debug((Supplier<?>) () -> "parsing exception for role [" + finalRoleName + "]", e);
            } else {
                logger.error(e.getMessage() + ". skipping role...");
            }
        } catch (IOException | XContentParseException e) {
            if (roleName != null) {
                final String finalRoleName = roleName;
                logger.error(() -> format("invalid role definition [%s] in roles file [%s]. skipping role...", finalRoleName, path), e);
            } else {
                logger.error(() -> format("invalid role definition [%s] in roles file [%s]. skipping role...", segment, path), e);
            }
        }
        return null;
    }

    @Nullable
    private static RoleDescriptor checkDescriptor(
        RoleDescriptor descriptor,
        Path path,
        Logger logger,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        FileRoleValidator roleValidator
    ) {
        String roleName = descriptor.getName();
        // first check if FLS/DLS is enabled on the role...
        if (descriptor.isUsingDocumentOrFieldLevelSecurity()) {
            if (XPackSettings.DLS_FLS_ENABLED.get(settings) == false) {
                logger.error(
                    "invalid role definition [{}] in roles file [{}]. document and field level security is not "
                        + "enabled. set [{}] to [true] in the configuration file. skipping role...",
                    roleName,
                    path.toAbsolutePath(),
                    XPackSettings.DLS_FLS_ENABLED.getKey()
                );
                return null;
            } else {
                try {
                    DLSRoleQueryValidator.validateQueryField(descriptor.getIndicesPrivileges(), xContentRegistry);
                } catch (ElasticsearchException | IllegalArgumentException e) {
                    logger.error(
                        () -> format(
                            "invalid role definition [%s] in roles file [%s]. failed to validate query field. skipping role...",
                            roleName,
                            path.toAbsolutePath()
                        ),
                        e
                    );
                    return null;
                }
            }
        }
        ActionRequestValidationException ex = roleValidator.validatePredefinedRole(descriptor);
        if (ex != null) {
            throw ex;
        }
        Validation.Error validationError = Validation.Roles.validateRoleDescription(descriptor.getDescription());
        if (validationError != null) {
            logger.error(
                "invalid role definition [{}] in roles file [{}]. invalid description - {}. skipping role...",
                roleName,
                path.toAbsolutePath(),
                validationError
            );
            return null;
        }
        return descriptor;
    }

    private static List<String> roleSegments(Path path) throws IOException {
        List<String> segments = new ArrayList<>();
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

    private class FileListener implements FileChangesListener {

        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            onFileChanged(file);
        }

        @Override
        public synchronized void onFileChanged(Path file) {
            if (file.equals(FileRolesStore.this.file)) {
                final Map<String, RoleDescriptor> previousPermissions = permissions;
                try {
                    permissions = parseFile(file, logger, settings, licenseState, xContentRegistry, roleValidator);
                } catch (Exception e) {
                    logger.error(
                        () -> format("could not reload roles file [%s]. Current roles remain unmodified", file.toAbsolutePath()),
                        e
                    );
                    return;
                }

                final Set<String> changedOrMissingRoles = Sets.difference(previousPermissions.entrySet(), permissions.entrySet())
                    .stream()
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
                final Set<String> addedRoles = Sets.difference(permissions.keySet(), previousPermissions.keySet());
                final Set<String> changedRoles = unmodifiableSet(Sets.union(changedOrMissingRoles, addedRoles));
                if (changedRoles.isEmpty() == false) {
                    logger.info("updated roles (roles file [{}] {})", file.toAbsolutePath(), Files.exists(file) ? "changed" : "removed");
                    listeners.forEach(c -> c.accept(changedRoles));
                }
            }
        }
    }
}
