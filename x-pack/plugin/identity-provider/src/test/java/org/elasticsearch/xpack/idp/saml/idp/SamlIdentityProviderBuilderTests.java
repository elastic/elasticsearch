/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderResolver;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults;
import org.elasticsearch.xpack.idp.saml.sp.WildcardServiceProviderResolver;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.mockito.Mockito;
import org.opensaml.security.x509.X509Credential;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ALLOWED_NAMEID_FORMATS;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_CONTACT_EMAIL;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_CONTACT_GIVEN_NAME;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_CONTACT_SURNAME;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ENTITY_ID;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ORGANIZATION_DISPLAY_NAME;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ORGANIZATION_NAME;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ORGANIZATION_URL;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_SLO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_SLO_REDIRECT_ENDPOINT;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_SSO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_SSO_REDIRECT_ENDPOINT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;
import static org.opensaml.saml.saml2.core.NameIDType.PERSISTENT;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class SamlIdentityProviderBuilderTests extends IdpSamlTestCase {

    public void testAllSettings() throws Exception {
        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyPath = getDataPath("signing1.key");
        final Path destSigningKeyPath = dir.resolve("signing1.key");
        final PrivateKey signingKey = PemUtils.readPrivateKey(signingKeyPath, () -> null);
        final Path signingCertPath = getDataPath("signing1.crt");
        final Path destSigningCertPath = dir.resolve("signing1.crt");
        final X509Certificate signingCert = CertParsingUtils.readX509Certificates(List.of(signingCertPath))[0];
        Files.copy(signingKeyPath, destSigningKeyPath);
        Files.copy(signingCertPath, destSigningCertPath);

        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect")
            .put(IDP_SSO_POST_ENDPOINT.getKey(), "https://idp.org/sso/post")
            .put(IDP_SLO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/slo/redirect")
            .put(IDP_SLO_POST_ENDPOINT.getKey(), "https://idp.org/slo/post")
            .putList(IDP_ALLOWED_NAMEID_FORMATS.getKey(), TRANSIENT)
            .put(IDP_ORGANIZATION_NAME.getKey(), "organization_name")
            .put(IDP_ORGANIZATION_DISPLAY_NAME.getKey(), "organization_display_name")
            .put(IDP_ORGANIZATION_URL.getKey(), "https://idp.org")
            .put(IDP_CONTACT_GIVEN_NAME.getKey(), "Tony")
            .put(IDP_CONTACT_SURNAME.getKey(), "Stark")
            .put(IDP_CONTACT_EMAIL.getKey(), "tony@starkindustries.com")
            .put(ServiceProviderDefaults.APPLICATION_NAME_SETTING.getKey(), "my_super_idp")
            .put(ServiceProviderDefaults.NAMEID_FORMAT_SETTING.getKey(), PERSISTENT)
            .put(ServiceProviderDefaults.AUTHN_EXPIRY_SETTING.getKey(), "2m")
            .put("xpack.idp.signing.key", destSigningKeyPath)
            .put("xpack.idp.signing.certificate", destSigningCertPath)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final SamlServiceProviderResolver serviceResolver = Mockito.mock(SamlServiceProviderResolver.class);
        final WildcardServiceProviderResolver wildcardResolver = Mockito.mock(WildcardServiceProviderResolver.class);
        final ServiceProviderDefaults defaults = ServiceProviderDefaults.forSettings(settings);
        final SamlIdentityProvider idp = SamlIdentityProvider.builder(serviceResolver, wildcardResolver)
            .fromSettings(env)
            .serviceProviderDefaults(defaults)
            .build();
        assertThat(idp.getEntityId(), equalTo("urn:elastic:cloud:idp"));
        assertThat(idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI).toString(), equalTo("https://idp.org/sso/redirect"));
        assertThat(idp.getSingleSignOnEndpoint(SAML2_POST_BINDING_URI).toString(), equalTo("https://idp.org/sso/post"));
        assertThat(idp.getSingleLogoutEndpoint(SAML2_REDIRECT_BINDING_URI).toString(), equalTo("https://idp.org/slo/redirect"));
        assertThat(idp.getSingleLogoutEndpoint(SAML2_POST_BINDING_URI).toString(), equalTo("https://idp.org/slo/post"));
        assertThat(idp.getAllowedNameIdFormats(), hasSize(1));
        assertThat(idp.getAllowedNameIdFormats(), Matchers.contains(TRANSIENT));
        assertThat(idp.getOrganization(), equalTo(new SamlIdentityProvider.OrganizationInfo("organization_name",
            "organization_display_name", "https://idp.org")));
        assertThat(idp.getServiceProviderDefaults().applicationName, equalTo("my_super_idp"));
        assertThat(idp.getServiceProviderDefaults().nameIdFormat, equalTo(PERSISTENT));
        assertThat(idp.getServiceProviderDefaults().authenticationExpiry, equalTo(Duration.standardMinutes(2)));
        assertThat(idp.getSigningCredential().getEntityCertificate(), equalTo(signingCert));
        assertThat(idp.getSigningCredential().getPrivateKey(), equalTo(signingKey));
    }

    public void testMissingCredentials() {
        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect")
            .put(IDP_SSO_POST_ENDPOINT.getKey(), "https://idp.org/sso/post")
            .put(IDP_SLO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/slo/redirect")
            .put(IDP_SLO_POST_ENDPOINT.getKey(), "https://idp.org/slo/post")
            .put(IDP_ALLOWED_NAMEID_FORMATS.getKey(), TRANSIENT)
            .put(IDP_ORGANIZATION_NAME.getKey(), "organization_name")
            .put(IDP_ORGANIZATION_DISPLAY_NAME.getKey(), "organization_display_name")
            .put(IDP_ORGANIZATION_URL.getKey(), "https://idp.org")
            .put(IDP_CONTACT_GIVEN_NAME.getKey(), "Tony")
            .put(IDP_CONTACT_SURNAME.getKey(), "Stark")
            .put(IDP_CONTACT_EMAIL.getKey(), "tony@starkindustries.com")
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final SamlServiceProviderResolver serviceResolver = Mockito.mock(SamlServiceProviderResolver.class);
        final WildcardServiceProviderResolver wildcardResolver = Mockito.mock(WildcardServiceProviderResolver.class);
        final ServiceProviderDefaults defaults = new ServiceProviderDefaults(
            randomAlphaOfLengthBetween(4, 8), randomFrom(TRANSIENT, PERSISTENT),
            Duration.standardMinutes(randomIntBetween(2, 90)));
        IllegalArgumentException e = LuceneTestCase.expectThrows(IllegalArgumentException.class,
            () -> SamlIdentityProvider.builder(serviceResolver, wildcardResolver)
                .fromSettings(env)
                .serviceProviderDefaults(defaults)
                .build());
        assertThat(e, instanceOf(ValidationException.class));
        assertThat(e.getMessage(), containsString("Signing credential must be specified"));
    }

    public void testDefaultAllowedNameIdFormats() throws Exception {
        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyPath = getDataPath("signing1.key");
        final Path destSigningKeyPath = dir.resolve("signing1.key");
        final Path signingCertPath = getDataPath("signing1.crt");
        final Path destSigningCertPath = dir.resolve("signing1.crt");
        Files.copy(signingKeyPath, destSigningKeyPath);
        Files.copy(signingCertPath, destSigningCertPath);

        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect")
            .put(IDP_SSO_POST_ENDPOINT.getKey(), "https://idp.org/sso/post")
            .put(IDP_SLO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/slo/redirect")
            .put(IDP_SLO_POST_ENDPOINT.getKey(), "https://idp.org/slo/post")
            .put(IDP_ALLOWED_NAMEID_FORMATS.getKey(), TRANSIENT)
            .put(IDP_ORGANIZATION_NAME.getKey(), "organization_name")
            .put(IDP_ORGANIZATION_DISPLAY_NAME.getKey(), "organization_display_name")
            .put(IDP_ORGANIZATION_URL.getKey(), "https://idp.org")
            .put(IDP_CONTACT_GIVEN_NAME.getKey(), "Tony")
            .put(IDP_CONTACT_SURNAME.getKey(), "Stark")
            .put(IDP_CONTACT_EMAIL.getKey(), "tony@starkindustries.com")
            .put(ServiceProviderDefaults.APPLICATION_NAME_SETTING.getKey(), "my_super_idp")
            .put(ServiceProviderDefaults.NAMEID_FORMAT_SETTING.getKey(), PERSISTENT)
            .put(ServiceProviderDefaults.AUTHN_EXPIRY_SETTING.getKey(), "2m")
            .put("xpack.idp.signing.key", destSigningKeyPath)
            .put("xpack.idp.signing.certificate", destSigningCertPath)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final SamlServiceProviderResolver serviceResolver = Mockito.mock(SamlServiceProviderResolver.class);
        final WildcardServiceProviderResolver wildcardResolver = Mockito.mock(WildcardServiceProviderResolver.class);
        final ServiceProviderDefaults defaults = ServiceProviderDefaults.forSettings(settings);
        final SamlIdentityProvider idp = SamlIdentityProvider.builder(serviceResolver, wildcardResolver)
            .fromSettings(env)
            .serviceProviderDefaults(defaults)
            .build();
        assertThat(idp.getAllowedNameIdFormats(), hasSize(1));
        assertThat(idp.getAllowedNameIdFormats(), Matchers.contains(TRANSIENT));
    }

    public void testConfigurationWithForbiddenAllowedNameIdFormats() throws Exception {
        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyPath = getDataPath("signing1.key");
        final Path destSigningKeyPath = dir.resolve("signing1.key");
        final Path signingCertPath = getDataPath("signing1.crt");
        final Path destSigningCertPath = dir.resolve("signing1.crt");
        Files.copy(signingKeyPath, destSigningKeyPath);
        Files.copy(signingCertPath, destSigningCertPath);

        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect")
            .put(IDP_SSO_POST_ENDPOINT.getKey(), "https://idp.org/sso/post")
            .put(IDP_SLO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/slo/redirect")
            .put(IDP_SLO_POST_ENDPOINT.getKey(), "https://idp.org/slo/post")
            .putList(IDP_ALLOWED_NAMEID_FORMATS.getKey(), TRANSIENT, PERSISTENT)
            .put(IDP_ORGANIZATION_NAME.getKey(), "organization_name")
            .put(IDP_ORGANIZATION_DISPLAY_NAME.getKey(), "organization_display_name")
            .put(IDP_ORGANIZATION_URL.getKey(), "https://idp.org")
            .put(IDP_CONTACT_GIVEN_NAME.getKey(), "Tony")
            .put(IDP_CONTACT_SURNAME.getKey(), "Stark")
            .put(IDP_CONTACT_EMAIL.getKey(), "tony@starkindustries.com")
            .put(ServiceProviderDefaults.APPLICATION_NAME_SETTING.getKey(), "my_super_idp")
            .put(ServiceProviderDefaults.NAMEID_FORMAT_SETTING.getKey(), PERSISTENT)
            .put(ServiceProviderDefaults.AUTHN_EXPIRY_SETTING.getKey(), "2m")
            .put("xpack.idp.signing.key", destSigningKeyPath)
            .put("xpack.idp.signing.certificate", destSigningCertPath)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final SamlServiceProviderResolver serviceResolver = Mockito.mock(SamlServiceProviderResolver.class);
        final WildcardServiceProviderResolver wildcardResolver = Mockito.mock(WildcardServiceProviderResolver.class);
        final ServiceProviderDefaults defaults = ServiceProviderDefaults.forSettings(settings);
        IllegalArgumentException e = LuceneTestCase.expectThrows(IllegalArgumentException.class, () ->
            SamlIdentityProvider.builder(serviceResolver, wildcardResolver).fromSettings(env).serviceProviderDefaults(defaults).build());
        assertThat(e.getMessage(), containsString("are not valid NameID formats. Allowed values are"));
        assertThat(e.getMessage(), containsString(PERSISTENT));
    }

    public void testInvalidSsoEndpoint() {
        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "not a url")
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final SamlServiceProviderResolver serviceResolver = Mockito.mock(SamlServiceProviderResolver.class);
        final WildcardServiceProviderResolver wildcardResolver = Mockito.mock(WildcardServiceProviderResolver.class);
        IllegalArgumentException e = LuceneTestCase.expectThrows(IllegalArgumentException.class,
            () -> SamlIdentityProvider.builder(serviceResolver, wildcardResolver).fromSettings(env).build());
        assertThat(e.getMessage(), containsString(IDP_SSO_REDIRECT_ENDPOINT.getKey()));
        assertThat(e.getMessage(), containsString("Not a valid URL"));
    }

    public void testMissingSsoRedirectEndpoint() {
        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_ORGANIZATION_NAME.getKey(), "The Organization")
            .put(IDP_ORGANIZATION_URL.getKey(), "https://idp.org")
            .put(IDP_CONTACT_EMAIL.getKey(), "tony@starkindustries.com")
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final SamlServiceProviderResolver serviceResolver = Mockito.mock(SamlServiceProviderResolver.class);
        final WildcardServiceProviderResolver wildcardResolver = Mockito.mock(WildcardServiceProviderResolver.class);
        IllegalArgumentException e = LuceneTestCase.expectThrows(IllegalArgumentException.class,
            () -> SamlIdentityProvider.builder(serviceResolver, wildcardResolver).fromSettings(env).build());
        assertThat(e.getMessage(), containsString(IDP_SSO_REDIRECT_ENDPOINT.getKey()));
        assertThat(e.getMessage(), containsString("is required"));
    }

    public void testMalformedOrganizationUrl() {
        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect")
            .put(IDP_ORGANIZATION_URL.getKey(), "The Org")
            .put(IDP_CONTACT_EMAIL.getKey(), "tony@starkindustries.com")
            .put(IDP_ORGANIZATION_NAME.getKey(), "The Organization")
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final SamlServiceProviderResolver serviceResolver = Mockito.mock(SamlServiceProviderResolver.class);
        final WildcardServiceProviderResolver wildcardResolver = Mockito.mock(WildcardServiceProviderResolver.class);
        IllegalArgumentException e = LuceneTestCase.expectThrows(IllegalArgumentException.class,
            () -> SamlIdentityProvider.builder(serviceResolver, wildcardResolver).fromSettings(env).build());
        assertThat(e.getMessage(), containsString(IDP_ORGANIZATION_URL.getKey()));
        assertThat(e.getMessage(), containsString("Not a valid URL"));
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
        final X509Certificate signingCert = CertParsingUtils.readX509Certificates(List.of(signingCertPath))[0];
        Files.copy(signingKeyPath, destSigningKeyPath);
        Files.copy(signingCertPath, destSigningCertPath);
        builder.put("xpack.idp.signing.key", destSigningKeyPath);
        builder.put("xpack.idp.signing.certificate", destSigningCertPath);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final X509Credential credential = SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.signing.");

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
        final X509Credential credential = SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.signing.");
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
        final Path signingKeyPath = getDataPath("signing1.key");
        final PrivateKey signingKey = PemUtils.readPrivateKey(signingKeyPath, () -> null);
        final X509Credential credential = SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.signing.");
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.signing."));
        assertThat(e, throwableWithMessage(
            "The configured credential [xpack.idp.signing.keystore] with alias [some-other] is not a valid signing key"
                + " - There is no private key available for this credential"));
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
        final X509Credential credential = SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.signing.");
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.signing."));
        assertThat(e, throwableWithMessage(
            "The configured credential [xpack.idp.signing.keystore] with alias [some-other] is not a valid signing key"
                + " - There is no private key available for this credential"));
    }

    public void testCreateMetadataSigningCredentialFromΚeystoreWithSingleEntry() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");

        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyStorePath = getDataPath("signing.p12");
        final Path destSigningKeyStorePath = dir.resolve("signing.p12");
        Files.copy(signingKeyStorePath, destSigningKeyStorePath);
        builder.put("xpack.idp.metadata_signing.keystore.path", destSigningKeyStorePath);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.metadata_signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final X509Credential credential = SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.metadata_signing.");
        final Path signingKeyPath = getDataPath("signing1.key");
        final PrivateKey signingKey = PemUtils.readPrivateKey(signingKeyPath, () -> null);

        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(signingKey));
    }

    public void testCreateMetadataSigningCredentialFromKeyStoreWithSingleEntryAndConfiguredAlias() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");

        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyStorePath = getDataPath("signing.p12");
        final Path destSigningKeyStorePath = dir.resolve("signing.p12");
        Files.copy(signingKeyStorePath, destSigningKeyStorePath);
        builder.put("xpack.idp.metadata_signing.keystore.path", destSigningKeyStorePath);
        builder.put("xpack.idp.metadata_signing.keystore.alias", "signing1");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.metadata_signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final Path signingKeyPath = getDataPath("signing1.key");
        final PrivateKey signingKey = PemUtils.readPrivateKey(signingKeyPath, () -> null);
        final X509Credential credential = SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.metadata_signing.");
        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(signingKey));
    }

    public void testCreateMetadataSigningCredentialFromKeyStoreWithSingleEntryButWrongAlias() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");

        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyStorePath = getDataPath("signing.p12");
        final Path destSigningKeyStorePath = dir.resolve("signing.p12");
        Files.copy(signingKeyStorePath, destSigningKeyStorePath);
        builder.put("xpack.idp.metadata_signing.keystore.path", destSigningKeyStorePath);
        builder.put("xpack.idp.metadata_signing.keystore.alias", "some-other");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.metadata_signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.metadata_signing."));
        assertThat(e, throwableWithMessage(
            "The configured credential [xpack.idp.metadata_signing.keystore] with alias [some-other] is not a valid signing key"
                + " - There is no private key available for this credential"));
    }

    public void testCreateMetadataSigningCredentialFromKeyStoreWithMultipleEntriesAndConfiguredAlias() throws Exception {
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
        builder.put("xpack.idp.metadata_signing.keystore.path", destSigningKeyStorePath);
        builder.put("xpack.idp.metadata_signing.keystore.alias", alias);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.metadata_signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final X509Credential credential = SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.metadata_signing.");
        final Path signingKeyPath = getDataPath(alias + ".key");
        final PrivateKey signingKey = PemUtils.readPrivateKey(signingKeyPath, () -> null);

        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(signingKey));
    }

    public void testCreateMetadataSigningCredentialFromKeyStoreWithMultipleEntriesButWrongAlias() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect");

        final Path dir = LuceneTestCase.createTempDir("signing");
        final Path signingKeyStorePath = getDataPath("multi_signing.p12");
        final Path destSigningKeyStorePath = dir.resolve("multi_signing.p12");
        Files.copy(signingKeyStorePath, destSigningKeyStorePath);
        builder.put("xpack.idp.metadata_signing.keystore.path", destSigningKeyStorePath);
        builder.put("xpack.idp.metadata_signing.keystore.alias", "some-other");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.idp.metadata_signing.keystore.secure_password", "signing");
        builder.setSecureSettings(secureSettings);
        final Settings settings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> SamlIdentityProviderBuilder.buildSigningCredential(env, settings, "xpack.idp.metadata_signing."));
        assertThat(e, throwableWithMessage(
            "The configured credential [xpack.idp.metadata_signing.keystore] with alias [some-other] is not a valid signing key"
                + " - There is no private key available for this credential"));
    }

}
