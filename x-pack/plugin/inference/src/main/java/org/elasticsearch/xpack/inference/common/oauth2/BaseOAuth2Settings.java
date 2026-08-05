/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xpack.inference.common.ValidationResult;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Shared base for service-specific OAuth2 client-credentials settings (e.g. Azure OpenAI, OpenAI).
 * Holds the common {@link OAuth2Settings} (client id + scopes) and the all-or-none validation
 * rule; subclasses add their service-specific extra field (e.g. {@code tenant_id}, {@code token_url}) and its
 * serialization.
 */
public abstract class BaseOAuth2Settings implements ToXContentFragment, Writeable {

    protected static String requiredFieldsDescription(Set<String> requiredFields) {
        return Strings.format("OAuth2 requires the fields %s, to be set.", new TreeSet<>(requiredFields));
    }

    /**
     * Validates that either all or none of the OAuth2 fields are provided. If any field is
     * provided, then all fields must be provided, because OAuth2 requires them set together.
     *
     * @param oauth2Settings the result of parsing the common OAuth2 fields (client id, scopes)
     * @param extraField the service-specific field (only null-checked here); e.g. {@code tenant_id} or {@code token_url}
     * @param extraFieldName the name of {@code extraField}, used to build the missing-field error message
     * @param serviceDescription human-readable service name used in the error message
     * @param validationException accumulates any validation errors found
     * @return true if all fields are provided, false if none are provided or some are missing
     */
    protected static boolean validateFields(
        ValidationResult<OAuth2Settings> oauth2Settings,
        @Nullable Object extraField,
        String extraFieldName,
        String serviceDescription,
        ValidationException validationException
    ) {
        if (extraField == null) {
            if (oauth2Settings.isUndefined()) {
                return false;
            }
            addMissingFieldsValidationException(serviceDescription, Set.of(extraFieldName), validationException);
            return false;
        } else if (oauth2Settings.isUndefined()) {
            addMissingFieldsValidationException(serviceDescription, OAuth2Settings.REQUIRED_FIELDS, validationException);
            return false;
        }
        return oauth2Settings.isSuccess();
    }

    private static void addMissingFieldsValidationException(
        String serviceDescription,
        Set<String> missingFields,
        ValidationException validationException
    ) {
        validationException.addValidationError(
            Strings.format(
                "[%s] all %s OAuth2 fields must be provided together; missing: %s",
                ModelConfigurations.SERVICE_SETTINGS,
                serviceDescription,
                new TreeSet<>(missingFields)
            )
        );
    }

    protected final OAuth2Settings oAuth2Settings;

    protected BaseOAuth2Settings(OAuth2Settings oAuth2Settings) {
        this.oAuth2Settings = Objects.requireNonNull(oAuth2Settings);
    }

    public String clientId() {
        return oAuth2Settings.clientId();
    }

    public List<String> scopes() {
        return oAuth2Settings.scopes();
    }
}
