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
    default <T extends SecretSettings> T updateOnlyField(
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
                Strings.format("[%s] only [%s] can be updated for this secret, received: %s", scope, allowedField, disallowed)
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
