/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

public interface SecretSettings extends ToXContentObject, VersionedNamedWriteable {

    /**
     * Update the secret settings with the provided settings, combining them as necessary.
     *
     * @param newSecrets a <b>modifiable</b> map with the new secret settings
     * @return the updated secret settings
     */
    SecretSettings newSecretSettings(Map<String, Object> newSecrets);

    /**
     * Builds the canonical "exactly one field" validation message for a given settings scope and set of
     * acceptable field names. The fields are sorted alphabetically to ensure a deterministic, human-readable
     * output regardless of the input {@link Set} implementation.
     *
     * @param scope the settings scope name used as the message prefix (e.g. {@code "service_settings"})
     * @param fields    the acceptable field names
     * @return the formatted error message
     */
    static String exactlyOneFieldError(String scope, Set<String> fields) {
        return Strings.format("[%s] must have exactly one field of %s set", scope, new TreeSet<>(fields));
    }

    /**
     * Validates that {@code provided} contains exactly one field. Throws a {@link ValidationException} if the
     * map is empty or has more than one entry.
     *
     * @param provided  the map of fields supplied by the caller
     * @param scope the settings scope name used in the error message (e.g. {@code "service_settings"})
     * @param fields    the acceptable field names used in the error message
     */
    static void validateExactlyOneField(Map<String, ?> provided, String scope, Set<String> fields) {
        var errorMessage = exactlyOneFieldError(scope, fields);
        var validationException = new ValidationException();
        if (provided.isEmpty()) {
            validationException.addValidationError(errorMessage);
        } else if (provided.size() > 1) {
            validationException.addValidationError(errorMessage + ", received: " + provided.keySet());
        }
        validationException.throwIfValidationErrorsExist();
    }

    /**
     * Returns {@code this} (cast to {@code T}) when {@code allowedField} is unchanged, builds a new
     * instance via {@code factory} when it differs, and throws if any field other than {@code allowedField}
     * is present in {@code provided}.
     *
     * @param scope        the settings scope used in validation error messages (e.g. {@code "service_settings"})
     * @param allowedField the only field name permitted in an update
     * @param currentValue the current value of {@code allowedField}
     * @param provided     the non-empty map of fields supplied by the caller
     * @param factory      builds a new instance when the value changes
     */
    default <T extends SecretSettings> T updateExactlyOneField(
        String scope,
        String allowedField,
        SecureString currentValue,
        Map<String, SecureString> provided,
        Function<SecureString, T> factory
    ) {
        if (provided.size() > 1 || provided.containsKey(allowedField) == false) {
            var disallowed = new HashSet<>(provided.keySet());
            disallowed.remove(allowedField);
            throw new ValidationException().addValidationError(
                Strings.format("[%s] only the field [%s] can be updated for this secret, received: %s", scope, allowedField, disallowed)
            );
        }
        var newValue = provided.get(allowedField);
        if (Objects.equals(newValue, currentValue)) {
            @SuppressWarnings("unchecked")
            var self = (T) this;
            return self;
        }
        return factory.apply(newValue);
    }
}
