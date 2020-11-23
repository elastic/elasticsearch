/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public class OperatorUserDescriptor {
    private static final Logger logger = LogManager.getLogger(OperatorUserDescriptor.class);

    private final Path file;
    private volatile List<Group> groups;

    public OperatorUserDescriptor(Environment env, ResourceWatcherService watcherService) {
        this.file =  XPackPlugin.resolveConfigFile(env, "operator_users.yml");
        this.groups = parseFileLenient(file, logger);
        FileWatcher watcher = new FileWatcher(file.getParent());
        watcher.addListener(new OperatorUserDescriptor.FileListener());
        try {
            watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to start watching the operator users file [" + file.toAbsolutePath() + "]", e);
        }
    }

    public boolean isOperatorUser(Authentication authentication) {
        if (authentication.getUser().isRunAs()) {
            return false;
        } else if (User.isInternal(authentication.getUser())) {
            // Internal user are considered operator users
            return true;
        }

        return groups.stream().anyMatch(group -> {
            final Authentication.RealmRef realm = authentication.getSourceRealm();
            return group.usernames.contains(authentication.getUser().principal())
                && group.authenticationType == authentication.getAuthenticationType()
                && realm.getType().equals(group.realmType)
                && (realm.getType().equals(FileRealmSettings.TYPE) || realm.getName().equals(group.realmName));
        });
    }

    // Package private for tests
    List<Group> getGroups() {
        return groups;
    }

    public static final class Group {
        private final Set<String> usernames;
        private final String realmName;
        private final String realmType;
        private final Authentication.AuthenticationType authenticationType;

        public Group(Set<String> usernames) {
            this(usernames, null, FileRealmSettings.TYPE, Authentication.AuthenticationType.REALM);
        }

        public Group(Set<String> usernames, String realmName) {
            this(usernames, realmName, FileRealmSettings.TYPE, Authentication.AuthenticationType.REALM);
        }

        public Group(
            Set<String> usernames, String realmName, String realmType, Authentication.AuthenticationType authenticationType) {
            this.usernames = usernames;
            this.realmName = realmName;
            this.realmType = realmType;
            this.authenticationType = authenticationType;
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
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Group group = (Group) o;
            return usernames.equals(group.usernames) && Objects.equals(realmName,
                group.realmName) && realmType.equals(group.realmType) && authenticationType == group.authenticationType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(usernames, realmName, realmType, authenticationType);
        }
    }

    public static List<Group> parseFileLenient(Path path, Logger logger) {
        if (false == Files.exists(path)) {
            logger.debug("Skip reading operator user file since it does not exist");
            return List.of();
        }
        logger.debug("Reading operator users file [{}]", path.toAbsolutePath());
        try {
            return parseFile(path);
        } catch (IOException | RuntimeException e) {
            logger.error("Failed to parse operator_users file [" + path + "].", e);
            return List.of();
        }
    }

    public static List<Group> parseFile(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path, StandardOpenOption.READ)) {
            return parseConfig(in);
        }
    }

    public static List<Group> parseConfig(InputStream in) throws IOException {
        final List<Group> groups = new ArrayList<>();
        try (XContentParser parser = yamlParser(in)) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                final String categoryName = parser.currentName();
                if (false == AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(categoryName)) {
                    throw new IllegalArgumentException("Operator user config file must begin with operator, got [" + categoryName + "]");
                }
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    groups.add(parseOneGroup(parser));
                }
            }
        }
        return List.copyOf(groups);
    }

    private static Group parseOneGroup(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        String[] usernames = null;
        String realmName = null;
        String realmType = FileRealmSettings.TYPE;
        Authentication.AuthenticationType authenticationType = Authentication.AuthenticationType.REALM;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Fields.USERNAMES.match(currentFieldName, parser.getDeprecationHandler())) {
                usernames = XContentUtils.readStringArray(parser, false);
            } else if (Fields.REALM_NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    realmName = parser.text();
                } else {
                    throw mismatchedFieldException("realm_name", currentFieldName, "string", token);
                }
            } else if (Fields.REALM_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    realmType = parser.text();
                } else {
                    throw mismatchedFieldException("realm type", currentFieldName, "string", token);
                }
            } else if (Fields.AUTH_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    authenticationType = Authentication.AuthenticationType.valueOf(parser.text().toUpperCase(Locale.ROOT));
                } else {
                    throw mismatchedFieldException("authentication type", currentFieldName, "string", token);
                }
            } else {
                throw unexpectedFieldException("user", currentFieldName);
            }
        }
        if (usernames == null) {
            throw missingRequiredFieldException("usernames", Fields.USERNAMES.getPreferredName());
        }
        return new Group(Set.of(usernames), realmName, realmType, authenticationType);
    }

    private static ElasticsearchParseException mismatchedFieldException(String entityName,
                                                                        String fieldName, String expectedType,
                                                                        XContentParser.Token token) {
        return new ElasticsearchParseException(
            "failed to parse {}. " +
                "expected field [{}] value to be {}, but found an element of type [{}]",
            entityName, fieldName, expectedType, token);
    }

    private static ElasticsearchParseException missingRequiredFieldException(String entityName,
                                                                             String fieldName) {
        return new ElasticsearchParseException(
            "failed to parse {}. missing required [{}] field",
            entityName, fieldName);
    }

    private static ElasticsearchParseException unexpectedFieldException(String entityName,
                                                                        String fieldName) {
        return new ElasticsearchParseException(
            "failed to parse {}. unexpected field [{}]",
            entityName, fieldName);
    }

    private static XContentParser yamlParser(InputStream in) throws IOException {
        return XContentType.YAML.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, in);
    }

    public interface Fields {
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
            if (file.equals(OperatorUserDescriptor.this.file)) {
                List<Group> newGroups = parseFileLenient(file, logger);

                if (groups.equals(newGroups) == false) {
                    logger.info("operator users file [{}] changed. updating operator users...", file.toAbsolutePath());
                    groups = newGroups;
                }
            }
        }
    }
}
