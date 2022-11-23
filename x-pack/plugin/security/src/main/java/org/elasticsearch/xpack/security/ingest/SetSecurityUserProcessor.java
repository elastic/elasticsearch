/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
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

    private final Logger logger = LogManager.getLogger(SetSecurityUserProcessor.class);

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
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Authentication authentication = null;
        User user = null;
        if (this.securityContext != null) {
            authentication = securityContext.getAuthentication();
            if (authentication != null) {
                user = authentication.getEffectiveSubject().getUser();
            }
        }

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

        Object fieldValue = ingestDocument.getFieldValue(field, Object.class, true);

        @SuppressWarnings("unchecked")
        Map<String, Object> userObject = fieldValue instanceof Map ? (Map<String, Object>) fieldValue : new HashMap<>();

        for (Property property : properties) {
            switch (property) {
                case USERNAME:
                    if (user.principal() != null) {
                        userObject.put("username", user.principal());
                    }
                    break;
                case FULL_NAME:
                    if (user.fullName() != null) {
                        userObject.put("full_name", user.fullName());
                    }
                    break;
                case EMAIL:
                    if (user.email() != null) {
                        userObject.put("email", user.email());
                    }
                    break;
                case ROLES:
                    if (user.roles() != null && user.roles().length != 0) {
                        userObject.put("roles", Arrays.asList(user.roles()));
                    }
                    break;
                case METADATA:
                    if (user.metadata() != null && user.metadata().isEmpty() == false) {
                        userObject.put("metadata", user.metadata());
                    }
                    break;
                case API_KEY:
                    if (authentication.isApiKey()) {
                        final String apiKey = "api_key";
                        final Object existingApiKeyField = userObject.get(apiKey);
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> apiKeyField = existingApiKeyField instanceof Map
                            ? (Map<String, Object>) existingApiKeyField
                            : new HashMap<>();
                        if (authentication.getAuthenticatingSubject().getMetadata().containsKey(AuthenticationField.API_KEY_NAME_KEY)) {
                            apiKeyField.put(
                                "name",
                                authentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_NAME_KEY)
                            );
                        }
                        if (authentication.getAuthenticatingSubject().getMetadata().containsKey(AuthenticationField.API_KEY_ID_KEY)) {
                            apiKeyField.put(
                                "id",
                                authentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_ID_KEY)
                            );
                        }
                        final Map<String, Object> apiKeyMetadata = ApiKeyService.getApiKeyMetadata(authentication);
                        if (false == apiKeyMetadata.isEmpty()) {
                            apiKeyField.put("metadata", apiKeyMetadata);
                        }
                        if (false == apiKeyField.isEmpty()) {
                            userObject.put(apiKey, apiKeyField);
                        }
                    }
                    break;
                case REALM:
                    final String realmKey = "realm";
                    final Object existingRealmField = userObject.get(realmKey);
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> realmField = existingRealmField instanceof Map
                        ? (Map<String, Object>) existingRealmField
                        : new HashMap<>();

                    final Object realmName = ApiKeyService.getCreatorRealmName(authentication);
                    if (realmName != null) {
                        realmField.put("name", realmName);
                    }
                    final Object realmType = ApiKeyService.getCreatorRealmType(authentication);
                    if (realmType != null) {
                        realmField.put("type", realmType);
                    }
                    if (false == realmField.isEmpty()) {
                        userObject.put(realmKey, realmField);
                    }
                    break;
                case AUTHENTICATION_TYPE:
                    if (authentication.getAuthenticationType() != null) {
                        userObject.put("authentication_type", authentication.getAuthenticationType().toString());
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported property [" + property + "]");
            }
        }
        ingestDocument.setFieldValue(field, userObject);
        return ingestDocument;
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
            Map<String, Object> config
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
