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

import java.util.Map;

public class SnapshotEncryptedDataTests extends ESTestCase {

    // --- fromMap ---

    public void testFromMapMissingTypeThrows() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.fromMap(Map.of("password", "correcthorsebatterystaple"))
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("type is required"));
    }

    public void testFromMapUnknownTypeThrows() {
        var e = expectThrows(IllegalArgumentException.class, () -> SnapshotEncryptedData.fromMap(Map.of("type", "hardware_token")));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("unknown encrypted_data.type"));
    }

    public void testFromMapSecureSettingRejected() {
        var e = expectThrows(IllegalArgumentException.class, () -> SnapshotEncryptedData.fromMap(Map.of("type", "secure_setting")));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("not supported yet"));
    }

    public void testFromMapUnknownFieldThrows() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.fromMap(Map.of("type", "password", "alien_field", "value"))
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("unknown field"));
    }

    public void testFromMapPasswordTypeNoPassword() {
        SnapshotEncryptedData result = SnapshotEncryptedData.fromMap(Map.of("type", "password"));
        assertEquals("password", result.type());
        assertNull(result.password());
    }

    public void testFromMapPasswordTypeWithPassword() {
        SnapshotEncryptedData result = SnapshotEncryptedData.fromMap(Map.of("type", "password", "password", "correcthorsebatterystaple"));
        assertEquals("password", result.type());
        assertNotNull(result.password());
        assertEquals("correcthorsebatterystaple", result.password().toString());
    }

    public void testFromMapPasswordTypeWithPasswordId() {
        SnapshotEncryptedData result = SnapshotEncryptedData.fromMap(
            Map.of("type", "password", "password", "a-very-long-passphrase-ok", "password_id", "my_id")
        );
        assertEquals("my_id", result.passwordId());
    }

    public void testFromMapNotAnObjectThrows() {
        var e = expectThrows(IllegalArgumentException.class, () -> SnapshotEncryptedData.fromMap("string_value"));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("should be an object"));
    }

    // --- validatePassword ---

    public void testValidatePasswordTooShort() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.validatePassword(new SecureString("tooshort".toCharArray()))
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("at least 15 characters"));
    }

    public void testValidatePasswordExactlyMinLength() {
        // 15 chars — must not throw
        SnapshotEncryptedData.validatePassword(new SecureString("aaaaaaaaaaaaaaa".toCharArray()));
    }

    public void testValidatePasswordNull() {
        var e = expectThrows(IllegalArgumentException.class, () -> SnapshotEncryptedData.validatePassword(null));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("at least 15 characters"));
    }

    public void testValidatePasswordSentinelRejected() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.validatePassword(new SecureString("::es_redacted::".toCharArray()))
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("redaction sentinel"));
    }

    // Template patterns — only purely bare templates are rejected; passwords containing them are fine.

    public void testValidateDollarBraceRejected() {
        // Exactly ${VAR}
        expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.validatePassword(new SecureString("${MY_SECRET_PASS}".toCharArray()))
        );
    }

    public void testValidateDollarBraceWithPrefixAccepted() {
        // Has a prefix — not a bare template
        SnapshotEncryptedData.validatePassword(new SecureString("x${MY_SECRET_PASS}extra".toCharArray()));
    }

    public void testValidateDollarVarRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.validatePassword(new SecureString("$MY_SECRET_PASS".toCharArray()))
        );
    }

    public void testValidateDollarVarWithSuffixAccepted() {
        SnapshotEncryptedData.validatePassword(new SecureString("prefix$MY_SECRET_PASSsuffix".toCharArray()));
    }

    public void testValidateMustacheRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.validatePassword(new SecureString("{{ .mySecret }}".toCharArray()))
        );
    }

    public void testValidateMustacheWithPrefixAccepted() {
        SnapshotEncryptedData.validatePassword(new SecureString("extra{{ .mySecret }}andmore".toCharArray()));
    }

    public void testValidatePercentVarRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.validatePassword(new SecureString("%MY_SECRET_PASS%".toCharArray()))
        );
    }

    public void testValidatePercentVarWithPrefixAccepted() {
        SnapshotEncryptedData.validatePassword(new SecureString("some%MY_SECRET_PASS%more".toCharArray()));
    }

    public void testValidateDollarBraceWithLeadingWhitespaceRejected() {
        // Leading/trailing whitespace only — still a bare template
        expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotEncryptedData.validatePassword(new SecureString("  ${MY_SECRET}  ".toCharArray()))
        );
    }

    public void testValidateNormalPasswordAccepted() {
        SnapshotEncryptedData.validatePassword(new SecureString("correcthorsebatterystaple".toCharArray()));
    }
}
