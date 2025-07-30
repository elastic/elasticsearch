/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * A processor that adds information of the current authenticated user to the document being ingested.
 */
public final class SetSecurityUserProcessor extends AbstractProcessor {

    public static final String TYPE = "set_security_user";

    private static final Logger logger = LogManager.getLogger(SetSecurityUserProcessor.class);
    private static final String API_KEY = "api_key";
    private static final String REALM_KEY = "realm";
    // a 'not found' sentinel value for use in getOrDefault calls below
    private static final Object NOT_FOUND = new Object();

    private final SecurityContext securityContext;
    private final Settings settings;
    private final String field;
    private final Set<Property> properties;

    public SetSecurityUserProcessor(
        String tag,
        String description,
        SecurityContext securityContext,
        Settings settings,
        String field,
        Set<Property> properties
    ) {
        super(tag, description);
        this.securityContext = securityContext;
        this.settings = Objects.requireNonNull(settings, "settings object cannot be null");
        if (XPackSettings.SECURITY_ENABLED.get(settings) == false) {
            logger.warn(
                "Creating processor [{}] (tag [{}]) on field [{}] but authentication is not currently enabled on this cluster "
                    + " - this processor is likely to fail at runtime if it is used",
                TYPE,
                tag,
                field
            );
        } else if (this.securityContext == null) {
            throw new IllegalArgumentException("Authentication is allowed on this cluster state, but there is no security context");
        }
        this.field = field;
        this.properties = properties;
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        final Authentication authentication = this.securityContext != null ? securityContext.getAuthentication() : null;
        final User user = authentication != null ? authentication.getEffectiveSubject().getUser() : null;

        if (user == null) {
            logger.debug(
                "Failed to find active user. SecurityContext=[{}] Authentication=[{}] User=[{}]",
                securityContext,
                authentication,
                user
            );
            if (XPackSettings.SECURITY_ENABLED.get(settings)) {
                // This shouldn't happen. If authentication is allowed (and active), then there _should_ always be an authenticated user.
                // If we ever see this error message, then one of our assumptions are wrong.
                throw new IllegalStateException(
                    "There is no authenticated user - the [" + TYPE + "] processor requires an authenticated user"
                );
            } else {
                throw new IllegalStateException(
                    "Security (authentication) is not enabled on this cluster, so there is no active user - "
                        + "the ["
                        + TYPE
                        + "] processor cannot be used without security"
                );
            }
        }

        final Map<String, Object> userObject = valueOrMapOfSize(document.getFieldValue(field, Object.class, true), properties.size());

        for (Property property : properties) {
            switch (property) {
                case USERNAME:
                    final String principal = user.principal();
                    if (principal != null) {
                        userObject.put("username", principal);
                    }
                    break;
                case FULL_NAME:
                    final String fullName = user.fullName();
                    if (fullName != null) {
                        userObject.put("full_name", fullName);
                    }
                    break;
                case EMAIL:
                    final String email = user.email();
                    if (email != null) {
                        userObject.put("email", email);
                    }
                    break;
                case ROLES:
                    final String[] roles = user.roles();
                    if (roles != null && roles.length != 0) {
                        userObject.put("roles", Arrays.asList(roles));
                    }
                    break;
                case METADATA:
                    final Map<String, Object> metadata = user.metadata();
                    if (metadata != null && metadata.isEmpty() == false) {
                        userObject.put("metadata", metadata);
                    }
                    break;
                case API_KEY:
                    if (authentication.isApiKey()) {
                        final Map<String, Object> apiKeyField = valueOrMapOfSize(userObject.get(API_KEY), 3);

                        final Map<String, Object> subjectMetadata = authentication.getAuthenticatingSubject().getMetadata();
                        final Object apiKeyName = subjectMetadata.getOrDefault(AuthenticationField.API_KEY_NAME_KEY, NOT_FOUND);
                        if (apiKeyName != NOT_FOUND) {
                            apiKeyField.put("name", apiKeyName);
                        }
                        final Object apiKeyId = subjectMetadata.getOrDefault(AuthenticationField.API_KEY_ID_KEY, NOT_FOUND);
                        if (apiKeyId != NOT_FOUND) {
                            apiKeyField.put("id", apiKeyId);
                        }
                        final Map<String, Object> apiKeyMetadata = ApiKeyService.getApiKeyMetadata(authentication);
                        if (false == apiKeyMetadata.isEmpty()) {
                            apiKeyField.put("metadata", apiKeyMetadata);
                        }

                        if (false == apiKeyField.isEmpty()) {
                            userObject.put(API_KEY, apiKeyField);
                        }
                    }
                    break;
                case REALM:
                    final Map<String, Object> realmField = valueOrMapOfSize(userObject.get(REALM_KEY), 2);

                    final Object realmName = ApiKeyService.getCreatorRealmName(authentication);
                    if (realmName != null) {
                        realmField.put("name", realmName);
                    }
                    final Object realmType = ApiKeyService.getCreatorRealmType(authentication);
                    if (realmType != null) {
                        realmField.put("type", realmType);
                    }

                    if (false == realmField.isEmpty()) {
                        userObject.put(REALM_KEY, realmField);
                    }
                    break;
                case AUTHENTICATION_TYPE:
                    final AuthenticationType authenticationType = authentication.getAuthenticationType();
                    if (authenticationType != null) {
                        userObject.put("authentication_type", authenticationType.toString());
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported property [" + property + "]");
            }
        }
        document.setFieldValue(field, userObject);
        return document;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> valueOrMapOfSize(@Nullable final Object value, final int size) {
        return value instanceof Map ? (Map<String, Object>) value : HashMap.newHashMap(size);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    Set<Property> getProperties() {
        return properties;
    }

    public static final class Factory implements Processor.Factory {

        private final Supplier<SecurityContext> securityContext;
        private final Settings settings;

        public Factory(Supplier<SecurityContext> securityContext, Settings settings) {
            this.securityContext = securityContext;
            this.settings = settings;
        }

        @Override
        public SetSecurityUserProcessor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            List<String> propertyNames = readOptionalList(TYPE, tag, config, "properties");
            Set<Property> properties;
            if (propertyNames != null) {
                properties = EnumSet.noneOf(Property.class);
                for (String propertyName : propertyNames) {
                    properties.add(Property.parse(tag, propertyName));
                }
            } else {
                properties = EnumSet.allOf(Property.class);
            }
            return new SetSecurityUserProcessor(tag, description, securityContext.get(), settings, field, properties);
        }
    }

    public enum Property {

        USERNAME,
        FULL_NAME,
        EMAIL,
        ROLES,
        METADATA,
        API_KEY,
        REALM,
        AUTHENTICATION_TYPE;

        static Property parse(String tag, String value) {
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                // not using the original exception as its message is confusing
                // (e.g. 'No enum constant SetSecurityUserProcessor.Property.INVALID')
                throw newConfigurationException(TYPE, tag, "properties", "Property value [" + value + "] is in valid");
            }
        }

    }

}
