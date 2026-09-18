/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OciGenAiSecretSettingsTests extends AbstractBWCWireSerializationTestCase<OciGenAiSecretSettings> {

    public static OciGenAiSecretSettings createRandom() {
        return new OciGenAiSecretSettings(
            randomSecureStringOfLength(20),
            randomSecureStringOfLength(20),
            randomSecureStringOfLength(47),
            randomSecureStringOfLength(64)
        );
    }

    public void testFromMap_Request_ParsesAllFields() {
        var settings = OciGenAiSecretSettings.fromMap(OciGenAiTestUtils.secretSettingsMap(), ConfigurationParseContext.REQUEST);

        assertThat(settings.tenancyId().toString(), is(OciGenAiTestUtils.TENANCY_ID));
        assertThat(settings.userId().toString(), is(OciGenAiTestUtils.USER_ID));
        assertThat(settings.fingerprint().toString(), is(OciGenAiTestUtils.FINGERPRINT));
        assertThat(settings.privateKey().toString(), is(OciGenAiTestUtils.privateKeyPem()));
        assertThat(settings.keyId(), is(OciGenAiTestUtils.keyId()));
    }

    public void testFromMap_ReturnsNull_WhenMapIsNull() {
        assertNull(OciGenAiSecretSettings.fromMap(null, ConfigurationParseContext.REQUEST));
    }

    public void testFromMap_Request_ThrowsWhenAFieldIsMissing() {
        var map = OciGenAiTestUtils.secretSettingsMap();
        map.remove(OciGenAiSecretSettings.TENANCY_ID);

        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiSecretSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            exception.getMessage(),
            containsString(
                Strings.format(
                    "[%s] does not contain the required setting [%s]",
                    ModelConfigurations.SERVICE_SETTINGS,
                    OciGenAiSecretSettings.TENANCY_ID
                )
            )
        );
    }

    public void testFromMap_Request_ThrowsWhenThePrivateKeyIsNotAValidPem() {
        var map = OciGenAiTestUtils.secretSettingsMap("not a pem");

        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiSecretSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("[service_settings] Invalid value for [private_key]."));
        assertThat(exception.getMessage(), containsString("Unsupported private key format"));
    }

    public void testFromMap_Request_ThrowsWhenThePrivateKeyIsEncrypted() {
        var map = OciGenAiTestUtils.secretSettingsMap("-----BEGIN ENCRYPTED PRIVATE KEY-----\nabcd\n-----END ENCRYPTED PRIVATE KEY-----");

        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiSecretSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("Passphrase protected private keys are not supported"));
    }

    public void testFromMap_Persistent_DoesNotValidateThePrivateKey() {
        var settings = OciGenAiSecretSettings.fromMap(
            OciGenAiTestUtils.secretSettingsMap("not a pem"),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.privateKey().toString(), is("not a pem"));
    }

    public void testFromMap_RemovesTheParsedFieldsFromTheMap() {
        var map = OciGenAiTestUtils.secretSettingsMap();
        map.put("other", "value");

        OciGenAiSecretSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(map, is(Map.of("other", "value")));
    }

    public void testNewSecretSettings_UpdatesTheProvidedFields() {
        var initial = OciGenAiTestUtils.createSecretSettings();
        var newKeyPem = OciGenAiTestUtils.toPkcs8Pem(OciGenAiTestUtils.generateKeyPair().getPrivate());

        var updated = (OciGenAiSecretSettings) initial.newSecretSettings(
            new HashMap<>(Map.of(OciGenAiSecretSettings.FINGERPRINT, "new-fingerprint", OciGenAiSecretSettings.PRIVATE_KEY, newKeyPem))
        );

        assertThat(updated.tenancyId(), is(initial.tenancyId()));
        assertThat(updated.userId(), is(initial.userId()));
        assertThat(updated.fingerprint().toString(), is("new-fingerprint"));
        assertThat(updated.privateKey().toString(), is(newKeyPem));
    }

    public void testNewSecretSettings_EmptyMap_ReturnsSameInstance() {
        var initial = OciGenAiTestUtils.createSecretSettings();

        assertThat(initial.newSecretSettings(new HashMap<>()), sameInstance(initial));
    }

    public void testNewSecretSettings_ThrowsWhenThePrivateKeyIsInvalid() {
        var initial = OciGenAiTestUtils.createSecretSettings();

        var exception = expectThrows(
            ValidationException.class,
            () -> initial.newSecretSettings(new HashMap<>(Map.of(OciGenAiSecretSettings.PRIVATE_KEY, "garbage")))
        );

        assertThat(exception.getMessage(), containsString("Invalid value for [private_key]"));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var settings = new OciGenAiSecretSettings(
            new SecureString("tenancy".toCharArray()),
            new SecureString("user".toCharArray()),
            new SecureString("fp".toCharArray()),
            new SecureString("pem".toCharArray())
        );

        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            {
                "tenancy_id": "tenancy",
                "user_id": "user",
                "fingerprint": "fp",
                "private_key": "pem"
            }
            """)));
    }

    @Override
    protected Writeable.Reader<OciGenAiSecretSettings> instanceReader() {
        return OciGenAiSecretSettings::new;
    }

    @Override
    protected OciGenAiSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OciGenAiSecretSettings mutateInstance(OciGenAiSecretSettings instance) throws IOException {
        return randomValueOtherThan(instance, OciGenAiSecretSettingsTests::createRandom);
    }

    @Override
    protected OciGenAiSecretSettings mutateInstanceForVersion(OciGenAiSecretSettings instance, TransportVersion version) {
        return instance;
    }
}
