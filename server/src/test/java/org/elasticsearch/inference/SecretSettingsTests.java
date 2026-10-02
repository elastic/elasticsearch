/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class SecretSettingsTests extends ESTestCase {

    private static final String FIELD = "secret_field";
    private static final String OTHER_FIELD = "other_field";
    private static final String SCOPE = "some_scope";
    private static final Set<String> FIELDS = Set.of(FIELD, OTHER_FIELD);

    // --- updateOnlyField tests ---

    public void testUpdateExactlyOneField_UnchangedValue_ReturnsSameInstance() {
        var value = new SecureString(randomAlphaOfLength(10).toCharArray());
        var settings = new TestSecretSettings(value);
        var result = settings.updateExactlyOneField(SCOPE, FIELD, value, Map.of(FIELD, value), TestSecretSettings::new);
        assertThat(result, sameInstance(settings));
    }

    public void testUpdateExactlyOneField_ChangedValue_BuildsNewInstanceViaFactory() {
        var str1 = randomAlphaOfLength(10);
        var str2 = randomValueOtherThan(str1, () -> randomAlphaOfLength(10));
        var value1 = new SecureString(str1.toCharArray());
        var value2 = new SecureString(str2.toCharArray());
        var settings = new TestSecretSettings(value1);
        var result = settings.updateExactlyOneField(SCOPE, FIELD, value1, Map.of(FIELD, value2), TestSecretSettings::new);
        assertThat(result, is(new TestSecretSettings(value2)));
    }

    public void testUpdateExactlyOneField_MultipleFields_ThrowsValidationException() {
        var value = new SecureString(randomAlphaOfLength(10).toCharArray());
        var settings = new TestSecretSettings(value);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> settings.updateExactlyOneField(
                SCOPE,
                FIELD,
                value,
                Map.of(
                    FIELD,
                    new SecureString(randomAlphaOfLength(5).toCharArray()),
                    OTHER_FIELD,
                    new SecureString(randomAlphaOfLength(5).toCharArray())
                ),
                TestSecretSettings::new
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("[%s] only the field [%s] can be updated for this secret, received: [%s]", SCOPE, FIELD, OTHER_FIELD)
            )
        );
    }

    public void testUpdateExactlyOneField_MissingAllowedField_ThrowsValidationException() {
        var value = new SecureString(randomAlphaOfLength(10).toCharArray());
        var settings = new TestSecretSettings(value);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> settings.updateExactlyOneField(
                SCOPE,
                FIELD,
                value,
                Map.of(OTHER_FIELD, new SecureString(randomAlphaOfLength(5).toCharArray())),
                TestSecretSettings::new
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("[%s] only the field [%s] can be updated for this secret, received: [%s]", SCOPE, FIELD, OTHER_FIELD)
            )
        );
    }

    // --- exactlyOneFieldError tests ---

    public void testExactlyOneFieldError_BuildsMessage() {
        var error = SecretSettings.exactlyOneFieldError(SCOPE, FIELDS);
        // fields are sorted alphabetically: other_field < secret_field
        assertThat(error, is(Strings.format("[%s] must have exactly one field of [%s, %s] set", SCOPE, OTHER_FIELD, FIELD)));
    }

    // --- validateExactlyOneField tests ---

    public void testValidateExactlyOneField_SingleField_DoesNotThrow() {
        SecretSettings.validateExactlyOneField(Map.of(FIELD, "value"), SCOPE, FIELDS);
    }

    public void testValidateExactlyOneField_EmptyMap_ThrowsWithBaseMessage() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> SecretSettings.validateExactlyOneField(Map.of(), SCOPE, FIELDS)
        );
        assertThat(thrownException.getMessage(), containsString(SecretSettings.exactlyOneFieldError(SCOPE, FIELDS)));
    }

    public void testValidateExactlyOneField_MultipleFields_ThrowsWithReceivedKeys() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> SecretSettings.validateExactlyOneField(new TreeMap<>(Map.of(FIELD, "val1", OTHER_FIELD, "val2")), SCOPE, FIELDS)
        );
        assertThat(
            thrownException.getMessage(),
            containsString(SecretSettings.exactlyOneFieldError(SCOPE, FIELDS) + Strings.format(", received: [%s, %s]", OTHER_FIELD, FIELD))
        );
    }

    /**
     * Minimal {@link SecretSettings} implementation used to exercise the interface's
     * default and static methods directly without depending on a service-specific class.
     */
    private record TestSecretSettings(SecureString value) implements SecretSettings {

        @Override
        public String getWriteableName() {
            return "test_secret_settings";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
            return this;
        }
    }
}
