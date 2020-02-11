/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
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

    private final SecurityContext securityContext;
    private final String field;
    private final Set<Property> properties;

    public
    SetSecurityUserProcessor(String tag, SecurityContext securityContext, String field, Set<Property> properties) {
        super(tag);
        this.securityContext = Objects.requireNonNull(securityContext, "security context must be provided");
        this.field = field;
        this.properties = properties;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            throw new IllegalStateException("No user authenticated, only use this processor via authenticated user");
        }
        User user = authentication.getUser();
        if (user == null) {
            throw new IllegalStateException("No user for authentication");
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
                    final String apiKey = "api_key";
                    final Object existingApiKeyField = userObject.get(apiKey);
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> apiKeyField =
                        existingApiKeyField instanceof Map ? (Map<String, Object>) existingApiKeyField : new HashMap<>();
                    Object apiKeyName = authentication.getMetadata().get(ApiKeyService.API_KEY_NAME_KEY);
                    if (apiKeyName != null) {
                        apiKeyField.put("name", apiKeyName);
                    }
                    Object apiKeyId = authentication.getMetadata().get(ApiKeyService.API_KEY_ID_KEY);
                    if (apiKeyId != null) {
                        apiKeyField.put("id", apiKeyId);
                    }
                    if (false == apiKeyField.isEmpty()) {
                        userObject.put(apiKey, apiKeyField);
                    }
                    break;
                case REALM:
                    final String realmKey = "realm";
                    final Object existingRealmField = userObject.get(realmKey);
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> realmField =
                        existingRealmField instanceof Map ? (Map<String, Object>) existingRealmField : new HashMap<>();

                    final Object realmName, realmType;
                    if (Authentication.AuthenticationType.API_KEY == authentication.getAuthenticationType()) {
                        realmName = authentication.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM_NAME);
                        realmType = authentication.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM_TYPE);
                    } else {
                        realmName = authentication.getSourceRealm().getName();
                        realmType = authentication.getSourceRealm().getType();
                    }
                    if (realmName != null) {
                        realmField.put("name", realmName);
                    }
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

        public Factory(Supplier<SecurityContext> securityContext) {
            this.securityContext = securityContext;
        }

        @Override
        public SetSecurityUserProcessor create(Map<String, Processor.Factory> processorFactories, String tag,
                                               Map<String, Object> config) throws Exception {
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
            return new SetSecurityUserProcessor(tag, securityContext.get(), field, properties);
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
