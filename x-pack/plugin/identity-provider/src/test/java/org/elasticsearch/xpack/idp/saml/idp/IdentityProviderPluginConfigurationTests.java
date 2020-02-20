/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.opensaml.security.x509.X509Credential;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;

import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ENTITY_ID;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SLO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SLO_REDIRECT_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SSO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SSO_REDIRECT_ENDPOINT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;

public class IdentityProviderPluginConfigurationTests extends IdpSamlTestCase {

    public void testAllSettings() {
        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect")
            .put(IDP_SSO_POST_ENDPOINT.getKey(), "https://idp.org/sso/post")
            .put(IDP_SLO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/slo/redirect")
            .put(IDP_SLO_POST_ENDPOINT.getKey(), "https://idp.org/slo/post")
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        CloudIdp idp = new CloudIdp(env, settings);
        Assert.assertThat(idp.getEntityId(), equalTo("urn:elastic:cloud:idp"));
        Assert.assertThat(idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI), equalTo("https://idp.org/sso/redirect"));
        Assert.assertThat(idp.getSingleSignOnEndpoint(SAML2_POST_BINDING_URI), equalTo("https://idp.org/sso/post"));
        Assert.assertThat(idp.getSingleLogoutEndpoint(SAML2_REDIRECT_BINDING_URI), equalTo("https://idp.org/slo/redirect"));
        Assert.assertThat(idp.getSingleLogoutEndpoint(SAML2_POST_BINDING_URI), equalTo("https://idp.org/slo/post"));
    }

    public void testInvalidSsoEndpoint() {
        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "not a uri")
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = LuceneTestCase.expectThrows(IllegalArgumentException.class, () -> new CloudIdp(env, settings));
        Assert.assertThat(e.getMessage(), Matchers.containsString(IDP_SSO_REDIRECT_ENDPOINT.getKey()));
        Assert.assertThat(e.getMessage(), Matchers.containsString("Not a valid URI"));
    }

    public void testCreateSigningCredentialFromPemFiles() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");

        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyPath = getDataPath("signing1.key");
        final Path destSigningKeyPath = dir.resolve("signing1.key");
        final PrivateKey signingKey = PemUtils.readPrivateKey(signingKeyPath, () -> null);
        final Path signingCertPath = getDataPath("signing1.crt");
        final Path destSigningCertPath = dir.resolve("signing1.crt");
        final X509Certificate signingCert = CertParsingUtils.readX509Certificates(Collections.singletonList(signingCertPath))[0];
        Files.copy(signingKeyPath, destSigningKeyPath);
        Files.copy(signingCertPath, destSigningCertPath);
        builder.put("xpack.idp.signing.key", destSigningKeyPath);
        builder.put("xpack.idp.signing.certificate", destSigningCertPath);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final X509Credential credential = CloudIdp.buildSigningCredential(env, settings);

        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(signingKey));
        assertThat(credential.getPublicKey(), equalTo(signingCert.getPublicKey()));
    }

    public void testCreateSigningCredentialFromΚeystoreWithSingleEntry() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");

        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyStorePath = getDataPath("signing.p12");
        final Path destSigningKeyStorePath = dir.resolve("signing.p12");
        Files.copy(signingKeyStorePath, destSigningKeyStorePath);
        builder.put("xpack.idp.signing.keystore.path", destSigningKeyStorePath);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final X509Credential credential = CloudIdp.buildSigningCredential(env, settings);
        final Path signingKeyPath = getDataPath("signing1.key");
        final PrivateKey signingKey = PemUtils.readPrivateKey(signingKeyPath, () -> null);

        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(signingKey));
    }

    public void testCreateSigningCredentialFromKeyStoreWithSingleEntryAndConfiguredAlias() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");

        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyStorePath = getDataPath("signing.p12");
        final Path destSigningKeyStorePath = dir.resolve("signing.p12");
        Files.copy(signingKeyStorePath, destSigningKeyStorePath);
        builder.put("xpack.idp.signing.keystore.path", destSigningKeyStorePath);
        builder.put("xpack.idp.signing.keystore.alias", "signing1");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final X509Credential credential = CloudIdp.buildSigningCredential(env, settings);
        final Path signingKeyPath = getDataPath("signing1.key");
        final PrivateKey signingKey = PemUtils.readPrivateKey(signingKeyPath, () -> null);

        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(signingKey));
    }

    public void testCreateSigningCredentialFromKeyStoreWithSingleEntryButWrongAlias() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");

        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyStorePath = getDataPath("signing.p12");
        final Path destSigningKeyStorePath = dir.resolve("signing.p12");
        Files.copy(signingKeyStorePath, destSigningKeyStorePath);
        builder.put("xpack.idp.signing.keystore.path", destSigningKeyStorePath);
        builder.put("xpack.idp.signing.keystore.alias", "some-other");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CloudIdp.buildSigningCredential(env, settings));
        assertThat(e.getMessage(), equalTo("The configured keystore for xpack.idp.signing.keystore does not have a private key associated" +
            " with alias [some-other]"));
    }

    public void testCreateSigningCredentialFromKeyStoreWithMultipleEntriesAndConfiguredAlias() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");
        final String ks = randomFrom("multi_signing.jks", "multi_signing.p12");
        final String alias = randomFrom("signing1", "signing2", "signing3", "signing4");
        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyStorePath = getDataPath(ks);
        final Path destSigningKeyStorePath = dir.resolve(ks);
        Files.copy(signingKeyStorePath, destSigningKeyStorePath);
        builder.put("xpack.idp.signing.keystore.path", destSigningKeyStorePath);
        builder.put("xpack.idp.signing.keystore.alias", alias);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final X509Credential credential = CloudIdp.buildSigningCredential(env, settings);
        final Path signingKeyPath = getDataPath(alias + ".key");
        final PrivateKey signingKey = PemUtils.readPrivateKey(signingKeyPath, () -> null);

        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(signingKey));
    }

    public void testCreateSigningCredentialFromKeyStoreWithMultipleEntriesButWrongAlias() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");

        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyStorePath = getDataPath("multi_signing.p12");
        final Path destSigningKeyStorePath = dir.resolve("multi_signing.p12");
        Files.copy(signingKeyStorePath, destSigningKeyStorePath);
        builder.put("xpack.idp.signing.keystore.path", destSigningKeyStorePath);
        builder.put("xpack.idp.signing.keystore.alias", "some-other");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CloudIdp.buildSigningCredential(env, settings));
        assertThat(e.getMessage(), equalTo("The configured keystore for xpack.idp.signing.keystore does not have a private key associated" +
            " with alias [some-other]"));
    }
}
