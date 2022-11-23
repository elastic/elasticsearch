/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED;

public class FileOperatorUsersStore {
    private static final Logger logger = LogManager.getLogger(FileOperatorUsersStore.class);

    private final Path file;
    private volatile OperatorUsersDescriptor operatorUsersDescriptor;

    public FileOperatorUsersStore(Environment env, ResourceWatcherService watcherService) {
        this.file = XPackPlugin.resolveConfigFile(env, "operator_users.yml");
        this.operatorUsersDescriptor = parseFile(this.file, logger);
        FileWatcher watcher = new FileWatcher(file.getParent(), true);
        watcher.addListener(new FileOperatorUsersStore.FileListener());
        try {
            watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to start watching the operator users file [" + file.toAbsolutePath() + "]", e);
        }
    }

    public boolean isOperatorUser(Authentication authentication) {
        // Other than realm name, other criteria must always be an exact match for the user to be an operator.
        // Realm name of a descriptor can be null. When it is null, it is ignored for comparison.
        // If not null, it will be compared exactly as well.
        // The special handling for realm name is because there can only be one file or native realm and it does
        // not matter what the name is.
        return operatorUsersDescriptor.groups.stream().anyMatch(group -> {
            final Authentication.RealmRef realm = authentication.getSourceRealm();
            final boolean match = group.usernames.contains(authentication.getEffectiveSubject().getUser().principal())
                && group.authenticationType == authentication.getAuthenticationType()
                && realm.getType().equals(group.realmType)
                && (group.realmName == null || group.realmName.equals(realm.getName()));
            logger.trace(
                "Matching user [{}] against operator rule [{}] is [{}]",
                authentication.getEffectiveSubject().getUser(),
                group,
                match
            );
            return match;
        });
    }

    // Package private for tests
    public OperatorUsersDescriptor getOperatorUsersDescriptor() {
        return operatorUsersDescriptor;
    }

    static final class OperatorUsersDescriptor {
        private final List<Group> groups;

        private OperatorUsersDescriptor(List<Group> groups) {
            this.groups = groups;
        }

        // Package private for tests
        List<Group> getGroups() {
            return groups;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OperatorUsersDescriptor that = (OperatorUsersDescriptor) o;
            return groups.equals(that.groups);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groups);
        }

        @Override
        public String toString() {
            return "OperatorUsersDescriptor{" + "groups=" + groups + '}';
        }
    }

    private static final OperatorUsersDescriptor EMPTY_OPERATOR_USERS_DESCRIPTOR = new OperatorUsersDescriptor(List.of());

    static final class Group {
        private static final Set<String> SINGLETON_REALM_TYPES = Set.of(
            FileRealmSettings.TYPE,
            NativeRealmSettings.TYPE,
            ReservedRealm.TYPE
        );

        private final Set<String> usernames;
        private final String realmName;
        private final String realmType;
        private final Authentication.AuthenticationType authenticationType;

        Group(Set<String> usernames) {
            this(usernames, null);
        }

        Group(Set<String> usernames, @Nullable String realmName) {
            this(usernames, realmName, null, null);
        }

        Group(Set<String> usernames, @Nullable String realmName, @Nullable String realmType, @Nullable String authenticationType) {
            this.usernames = usernames;
            this.realmName = realmName;
            this.realmType = realmType == null ? FileRealmSettings.TYPE : realmType;
            this.authenticationType = authenticationType == null
                ? Authentication.AuthenticationType.REALM
                : Authentication.AuthenticationType.valueOf(authenticationType.toUpperCase(Locale.ROOT));
            validate();
        }

        private void validate() {
            final ValidationException validationException = new ValidationException();
            if (false == FileRealmSettings.TYPE.equals(realmType)) {
                validationException.addValidationError("[realm_type] only supports [file]");
            }
            if (Authentication.AuthenticationType.REALM != authenticationType) {
                validationException.addValidationError("[auth_type] only supports [realm]");
            }
            if (realmName == null) {
                if (false == SINGLETON_REALM_TYPES.contains(realmType)) {
                    validationException.addValidationError(
                        "[realm_name] must be specified for realm types other than ["
                            + Strings.collectionToCommaDelimitedString(SINGLETON_REALM_TYPES)
                            + "]"
                    );
                }
            }
            if (false == validationException.validationErrors().isEmpty()) {
                throw validationException;
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Group[");
            sb.append("usernames=").append(usernames);
            if (realmName != null) {
                sb.append(", realm_name=").append(realmName);
            }
            if (realmType != null) {
                sb.append(", realm_type=").append(realmType);
            }
            if (authenticationType != null) {
                sb.append(", auth_type=").append(authenticationType.name().toLowerCase(Locale.ROOT));
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Group group = (Group) o;
            return usernames.equals(group.usernames)
                && Objects.equals(realmName, group.realmName)
                && realmType.equals(group.realmType)
                && authenticationType == group.authenticationType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(usernames, realmName, realmType, authenticationType);
        }
    }

    public static OperatorUsersDescriptor parseFile(Path file, Logger logger) {
        if (false == Files.exists(file)) {
            logger.warn(
                "Operator privileges [{}] is enabled, but operator user file does not exist. "
                    + "No user will be able to perform operator-only actions.",
                OPERATOR_PRIVILEGES_ENABLED.getKey()
            );
            return EMPTY_OPERATOR_USERS_DESCRIPTOR;
        } else {
            logger.debug("Reading operator users file [{}]", file.toAbsolutePath());
            try (InputStream in = Files.newInputStream(file, StandardOpenOption.READ)) {
                final OperatorUsersDescriptor operatorUsersDescriptor = parseConfig(in);
                logger.info(
                    "parsed [{}] group(s) with a total of [{}] operator user(s) from file [{}]",
                    operatorUsersDescriptor.groups.size(),
                    operatorUsersDescriptor.groups.stream().mapToLong(g -> g.usernames.size()).sum(),
                    file.toAbsolutePath()
                );
                logger.debug("operator user descriptor: [{}]", operatorUsersDescriptor);
                return operatorUsersDescriptor;
            } catch (IOException | RuntimeException e) {
                logger.error(() -> "Failed to parse operator users file [" + file + "].", e);
                throw new ElasticsearchParseException("Error parsing operator users file [{}]", e, file.toAbsolutePath());
            }
        }
    }

    // package method for testing
    static OperatorUsersDescriptor parseConfig(InputStream in) throws IOException {
        try (XContentParser parser = yamlParser(in)) {
            return OPERATOR_USER_PARSER.parse(parser, null);
        }
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Group, Void> GROUP_PARSER = new ConstructingObjectParser<>(
        "operator_privileges.operator.group",
        false,
        (Object[] arr) -> new Group(Set.copyOf((List<String>) arr[0]), (String) arr[1], (String) arr[2], (String) arr[3])
    );

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<OperatorUsersDescriptor, Void> OPERATOR_USER_PARSER = new ConstructingObjectParser<>(
        "operator_privileges.operator",
        false,
        (Object[] arr) -> new OperatorUsersDescriptor((List<Group>) arr[0])
    );

    static {
        GROUP_PARSER.declareStringArray(constructorArg(), Fields.USERNAMES);
        GROUP_PARSER.declareString(optionalConstructorArg(), Fields.REALM_NAME);
        GROUP_PARSER.declareString(optionalConstructorArg(), Fields.REALM_TYPE);
        GROUP_PARSER.declareString(optionalConstructorArg(), Fields.AUTH_TYPE);
        OPERATOR_USER_PARSER.declareObjectArray(constructorArg(), (parser, ignore) -> GROUP_PARSER.parse(parser, null), Fields.OPERATOR);
    }

    private static XContentParser yamlParser(InputStream in) throws IOException {
        return XContentType.YAML.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, in);
    }

    public interface Fields {
        ParseField OPERATOR = new ParseField("operator");
        ParseField USERNAMES = new ParseField("usernames");
        ParseField REALM_NAME = new ParseField("realm_name");
        ParseField REALM_TYPE = new ParseField("realm_type");
        ParseField AUTH_TYPE = new ParseField("auth_type");
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
        public void onFileChanged(Path file) {
            if (file.equals(FileOperatorUsersStore.this.file)) {
                final OperatorUsersDescriptor newDescriptor = parseFile(file, logger);
                if (operatorUsersDescriptor.equals(newDescriptor) == false) {
                    logger.info("operator users file [{}] changed. updating operator users...", file.toAbsolutePath());
                    operatorUsersDescriptor = newDescriptor;
                }
            }
        }
    }
}
