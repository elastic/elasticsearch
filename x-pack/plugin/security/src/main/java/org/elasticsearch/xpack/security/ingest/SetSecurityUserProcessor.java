/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * A processor that adds information of the current authenticated user to the document being ingested.
 */
public final class SetSecurityUserProcessor extends AbstractProcessor {

    public static final String TYPE = "set_security_user";

    private final ThreadContext threadContext;
    private final String field;
    private final Set<Property> properties;

    public SetSecurityUserProcessor(String tag, ThreadContext threadContext, String field, Set<Property> properties) {
        super(tag);
        this.threadContext = threadContext;
        this.field = field;
        this.properties = properties;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Authentication authentication = Authentication.getAuthentication(threadContext);
        if (authentication == null) {
            throw new IllegalStateException("No user authenticated, only use this processor via authenticated user");
        }
        User user = authentication.getUser();
        if (user == null) {
            throw new IllegalStateException("No user for authentication");
        }

        Map<String, Object> userObject = new HashMap<>();
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

        private final ThreadContext threadContext;

        public Factory(ThreadContext threadContext) {
            this.threadContext = threadContext;
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
            return new SetSecurityUserProcessor(tag, threadContext, field, properties);
        }
    }

    public enum Property {

        USERNAME,
        FULL_NAME,
        EMAIL,
        ROLES,
        METADATA;

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
