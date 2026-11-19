/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class SnapshotEncryptedDataTests extends ESTestCase {

    private static final List<String> TEMPLATES = List.of("${MY_SECRET_PASS}", "$MY_SECRET_PASS", "{{ .mySecret }}", "%MY_SECRET_PASS%");

    public void testFromMapMissingTypeThrows() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.fromMap(Map.of("password", "correcthorsebatterystaple"))
        );
        assertThat(e.getMessage(), containsString("type is required"));
    }

    public void testFromMapUnknownTypeThrows() {
        var e = expectThrows(IllegalArgumentException.class, () -> SnapshotEncryptedData.fromMap(Map.of("type", "hardware_token")));
        assertThat(e.getMessage(), containsString("unknown encrypted_data.type"));
    }

    public void testFromMapSecureSettingRejected() {
        var e = expectThrows(IllegalArgumentException.class, () -> SnapshotEncryptedData.fromMap(Map.of("type", "secure_setting")));
        assertThat(e.getMessage(), containsString("not supported yet"));
    }

    public void testFromMapUnknownFieldThrows() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.fromMap(Map.of("type", "password", "alien_field", "value"))
        );
        assertThat(e.getMessage(), containsString("unknown field"));
    }

    public void testFromMapPasswordTypeNoPassword() {
        SnapshotEncryptedData result = SnapshotEncryptedData.fromMap(Map.of("type", "password"));
        assertEquals("password", result.type());
        assertNull(result.password());
    }

    public void testFromMapPasswordTypeWithPassword() {
        SnapshotEncryptedData result = SnapshotEncryptedData.fromMap(
            Map.of("type", "password", "password", "correcthorsebatterystaple", "password_id", "my_id")
        );
        assertEquals("password", result.type());
        assertEquals("correcthorsebatterystaple", result.password().toString());
        assertEquals("my_id", result.passwordId());
    }

    public void testFromMapRejectsNonStringValues() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.fromMap(Map.of("type", "password", "password", 123456789012345L))
        );
        assertThat(e.getMessage(), containsString("encrypted_data.password must be a string"));
    }

    public void testValidatePasswordId() {
        new SnapshotEncryptedData(
            SnapshotEncryptedData.TYPE_PASSWORD,
            new SecureString("correcthorsebatterystaple".toCharArray()),
            "snapshot-2026_08"
        ).validate();
        for (String passwordId : List.of("", "has.dot", "has space", "a".repeat(129))) {
            var e = expectThrows(
                IllegalArgumentException.class,
                () -> new SnapshotEncryptedData(
                    SnapshotEncryptedData.TYPE_PASSWORD,
                    new SecureString("correcthorsebatterystaple".toCharArray()),
                    passwordId
                ).validate()
            );
            assertThat(e.getMessage(), containsString("encrypted_data.password_id must be"));
        }
    }

    public void testValidateRejectsUnsupportedTypes() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> new SnapshotEncryptedData(SnapshotEncryptedData.TYPE_SECURE_SETTING, null, null).validate()
        );
        assertThat(e.getMessage(), containsString("not supported yet"));
        for (String type : List.of(SnapshotEncryptedData.TYPE_NONE, "hardware_token")) {
            e = expectThrows(
                IllegalArgumentException.class,
                () -> new SnapshotEncryptedData(type, new SecureString("correcthorsebatterystaple".toCharArray()), null).validate()
            );
            assertThat(e.getMessage(), containsString("unknown encrypted_data.type [" + type + "]"));
        }
    }

    public void testFromMapNotAnObjectThrows() {
        var e = expectThrows(IllegalArgumentException.class, () -> SnapshotEncryptedData.fromMap("string_value"));
        assertThat(e.getMessage(), containsString("should be an object"));
    }

    public void testValidatePasswordTooShort() {
        var e = expectThrows(IllegalArgumentException.class, () -> validate("tooshort"));
        assertThat(e.getMessage(), containsString("at least 15 characters"));
    }

    public void testValidatePasswordExactlyMinLength() {
        validate("aaaaaaaaaaaaaaa");
    }

    public void testValidatePasswordTooLong() {
        validate("a".repeat(SnapshotEncryptedData.MAX_PASSWORD_LENGTH));
        var e = expectThrows(IllegalArgumentException.class, () -> validate("a".repeat(SnapshotEncryptedData.MAX_PASSWORD_LENGTH + 1)));
        assertThat(e.getMessage(), containsString("at most 1024 characters"));
    }

    public void testValidateRequiresPassword() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> new SnapshotEncryptedData(SnapshotEncryptedData.TYPE_PASSWORD, null, null).validate()
        );
        assertThat(e.getMessage(), containsString("password is required"));
    }

    public void testValidatePasswordSentinelRejected() {
        var e = expectThrows(IllegalArgumentException.class, () -> validate("::es_redacted::"));
        assertThat(e.getMessage(), containsString("redaction sentinel"));
    }

    public void testValidateBareTemplateRejected() {
        for (String template : TEMPLATES) {
            for (String password : List.of(template, "  " + template + "  ")) {
                expectThrows(IllegalArgumentException.class, () -> validate(password));
            }
        }
    }

    public void testValidateTemplateInsideLongerPasswordAccepted() {
        for (String template : TEMPLATES) {
            validate("prefix" + template + "suffix");
        }
    }

    private static void validate(String password) {
        new SnapshotEncryptedData(SnapshotEncryptedData.TYPE_PASSWORD, new SecureString(password.toCharArray()), null).validate();
    }
}
