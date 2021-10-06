/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import joptsimple.OptionSet;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.RequestedAttribute;
import org.opensaml.saml.saml2.metadata.SPSSODescriptor;
import org.opensaml.saml.security.impl.SAMLSignatureProfileValidator;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.credential.UsageType;
import org.opensaml.security.x509.BasicX509Credential;
import org.opensaml.xmlsec.keyinfo.KeyInfoSupport;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.X509Certificate;
import org.opensaml.xmlsec.signature.X509Data;
import org.opensaml.xmlsec.signature.support.SignatureValidator;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SamlMetadataCommandTests extends SamlTestCase {

    private KeyStoreWrapper keyStore;
    private KeyStoreWrapper passwordProtectedKeystore;

    @Before
    public void setup() throws Exception {
        SamlUtils.initialize(logger);
        this.keyStore = mock(KeyStoreWrapper.class);
        when(keyStore.isLoaded()).thenReturn(true);
        this.passwordProtectedKeystore = mock(KeyStoreWrapper.class);
        when(passwordProtectedKeystore.isLoaded()).thenReturn(true);
        when(passwordProtectedKeystore.hasPassword()).thenReturn(true);
        doNothing().when(passwordProtectedKeystore).decrypt("keystore-password".toCharArray());
        doThrow(new SecurityException("Provided keystore password was incorrect", new IOException()))
            .when(passwordProtectedKeystore).decrypt("wrong-password".toCharArray());
    }

    public void testDefaultOptions() throws Exception {
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        final Path certPath = getDataPath("saml.crt");
        final Path keyPath = getDataPath("saml.key");

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[0]);

        final boolean useSigningCredentials = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.my_saml.order", 1)
                .put(RealmSettings.PREFIX + "saml.my_saml.idp.entity_id", "https://okta.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.entity_id", "https://kibana.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.acs", "https://kibana.my.corp/saml/login")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.logout", "https://kibana.my.corp/saml/logout")
                .put(RealmSettings.PREFIX + "saml.my_saml.attributes.principal", "urn:oid:0.9.2342.19200300.100.1.1");
        if (useSigningCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.signing.certificate", certPath.toString())
                    .put(RealmSettings.PREFIX + "saml.my_saml.signing.key", keyPath.toString());
        }
        final Settings settings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = new MockTerminal();

        if (usedKeyStore.hasPassword()) {
            terminal.addSecretInput("keystore-password");
        }
        // What is the friendly name for "principal" attribute "urn:oid:0.9.2342.19200300.100.1.1" [default: principal]
        terminal.addTextInput("");

        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);

        assertThat(descriptor, notNullValue());
        assertThat(descriptor.getEntityID(), equalTo("https://kibana.my.corp/"));

        assertThat(descriptor.getRoleDescriptors(), iterableWithSize(1));
        assertThat(descriptor.getRoleDescriptors().get(0), instanceOf(SPSSODescriptor.class));
        final SPSSODescriptor spDescriptor = (SPSSODescriptor) descriptor.getRoleDescriptors().get(0);

        assertThat(spDescriptor.getAssertionConsumerServices(), iterableWithSize(1));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getLocation(), equalTo("https://kibana.my.corp/saml/login"));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).isDefault(), equalTo(true));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getIndex(), equalTo(1));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getBinding(), equalTo(SAMLConstants.SAML2_POST_BINDING_URI));

        assertThat(spDescriptor.getAttributeConsumingServices(), iterableWithSize(1));
        assertThat(spDescriptor.getAttributeConsumingServices().get(0).isDefault(), equalTo(true));
        assertThat(spDescriptor.getAttributeConsumingServices().get(0).getIndex(), equalTo(1));
        assertThat(spDescriptor.getAttributeConsumingServices().get(0).getRequestedAttributes(), iterableWithSize(1));
        final RequestedAttribute uidAttribute = spDescriptor.getAttributeConsumingServices().get(0).getRequestedAttributes().get(0);
        assertThat(uidAttribute.getName(), equalTo("urn:oid:0.9.2342.19200300.100.1.1"));
        assertThat(uidAttribute.getFriendlyName(), equalTo("principal"));

        assertThat(spDescriptor.getSingleLogoutServices(), iterableWithSize(1));
        assertThat(spDescriptor.getSingleLogoutServices().get(0).getLocation(), equalTo("https://kibana.my.corp/saml/logout"));
        assertThat(spDescriptor.getSingleLogoutServices().get(0).getBinding(), equalTo(SAMLConstants.SAML2_REDIRECT_BINDING_URI));

        assertThat(spDescriptor.isAuthnRequestsSigned(), equalTo(useSigningCredentials));
        assertThat(spDescriptor.getWantAssertionsSigned(), equalTo(true));

        if (useSigningCredentials) {
            assertThat(spDescriptor.getKeyDescriptors(), iterableWithSize(1));
            assertThat(spDescriptor.getKeyDescriptors().get(0).getUse(), equalTo(UsageType.SIGNING));
            assertThat(spDescriptor.getKeyDescriptors().get(0).getKeyInfo().getPGPDatas(), iterableWithSize(0));
            assertThat(spDescriptor.getKeyDescriptors().get(0).getKeyInfo().getMgmtDatas(), iterableWithSize(0));
            assertThat(spDescriptor.getKeyDescriptors().get(0).getKeyInfo().getSPKIDatas(), iterableWithSize(0));
            final List<X509Data> x509 = spDescriptor.getKeyDescriptors().get(0).getKeyInfo().getX509Datas();
            assertThat(x509, iterableWithSize(1));
            assertThat(x509.get(0).getX509Certificates(), iterableWithSize(1));
            final X509Certificate xmlCert = x509.get(0).getX509Certificates().get(0);
            assertThat(xmlCert.getValue(), startsWith("MIIDWDCCAkCgAwIBAgIVANRTZaFrK+Pz19O8TZsb3HSJmAWpMA0GCSqGSIb3DQEB"));

            // Verify that OpenSAML thinks the XML representation is the same as our input
            final java.security.cert.X509Certificate javaCert = KeyInfoSupport.getCertificate(xmlCert);
            assertThat(javaCert, equalTo(CertParsingUtils.readX509Certificate(certPath)));
        } else {
            assertThat(spDescriptor.getKeyDescriptors(), iterableWithSize(0));
        }
    }

    public void testFailIfMultipleRealmsExist() throws Exception {
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.saml_a.type", "saml")
                .put(RealmSettings.PREFIX + "saml.saml_a.sp.entity_id", "https://saml.a/")
                .put(RealmSettings.PREFIX + "saml.saml_a.sp.acs", "https://saml.a/")
                .put(RealmSettings.PREFIX + "saml.saml_b.type", "saml")
                .put(RealmSettings.PREFIX + "saml.saml_b.sp.entity_id", "https://saml.b/")
                .put(RealmSettings.PREFIX + "saml.saml_b.sp.acs", "https://saml.b/")
                .build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[0]);

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        final UserException userException = expectThrows(UserException.class, () -> command.buildEntityDescriptor(terminal, options, env));
        assertThat(userException.getMessage(), containsString("multiple SAML realms"));
        assertThat(terminal.getErrorOutput(), containsString("saml_a"));
        assertThat(terminal.getErrorOutput(), containsString("saml_b"));
        assertThat(terminal.getErrorOutput(), containsString("Use the -realm option"));
    }

    public void testSpecifyRealmNameAsParameter() throws Exception {
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.saml_a.order", 1)
                .put(RealmSettings.PREFIX + "saml.saml_a.type", "saml")
                .put(RealmSettings.PREFIX + "saml.saml_a.sp.entity_id", "https://saml.a/")
                .put(RealmSettings.PREFIX + "saml.saml_a.sp.acs", "https://saml.a/acs")
                .put(RealmSettings.PREFIX + "saml.saml_b.order", 2)
                .put(RealmSettings.PREFIX + "saml.saml_b.type", "saml")
                .put(RealmSettings.PREFIX + "saml.saml_b.sp.entity_id", "https://saml.b/")
                .put(RealmSettings.PREFIX + "saml.saml_b.sp.acs", "https://saml.b/acs")
                .build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[] {
                "-realm", "saml_b"
        });

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);

        assertThat(descriptor, notNullValue());
        assertThat(descriptor.getEntityID(), equalTo("https://saml.b/"));

        assertThat(descriptor.getRoleDescriptors(), iterableWithSize(1));
        assertThat(descriptor.getRoleDescriptors().get(0), instanceOf(SPSSODescriptor.class));
        final SPSSODescriptor spDescriptor = (SPSSODescriptor) descriptor.getRoleDescriptors().get(0);

        assertThat(spDescriptor.getAssertionConsumerServices(), iterableWithSize(1));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getLocation(), equalTo("https://saml.b/acs"));
    }

    public void testHandleAttributes() throws Exception {
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.saml1.order", 1)
                .put(RealmSettings.PREFIX + "saml.saml1.type", "saml")
                .put(RealmSettings.PREFIX + "saml.saml1.sp.entity_id", "https://saml.example.com/")
                .put(RealmSettings.PREFIX + "saml.saml1.sp.acs", "https://saml.example.com/")
                .put(RealmSettings.PREFIX + "saml.saml1.attributes.principal", "urn:oid:0.9.2342.19200300.100.1.1")
                .put(RealmSettings.PREFIX + "saml.saml1.attributes.name", "displayName")
                .build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[] {
                "-attribute", "urn:oid:0.9.2342.19200300.100.1.3",
                "-attribute", "groups"
        });

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        // What is the friendly name for command line attribute "urn:oid:0.9.2342.19200300.100.1.3" [default: none]
        terminal.addTextInput("mail");
        // What is the standard (urn) name for attribute "groups" (required)
        terminal.addTextInput("urn:oid:1.3.6.1.4.1.5923.1.5.1.1");
        // What is the standard (urn) name for "name" attribute "displayName" (required)
        terminal.addTextInput("urn:oid:2.16.840.1.113730.3.1.241");
        // What is the friendly name for "principal" "urn:oid:0.9.2342.19200300.100.1.1" [default: principal]
        terminal.addTextInput("uid");

        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);

        assertThat(descriptor, notNullValue());
        assertThat(descriptor.getEntityID(), equalTo("https://saml.example.com/"));

        assertThat(descriptor.getRoleDescriptors(), iterableWithSize(1));
        assertThat(descriptor.getRoleDescriptors().get(0), instanceOf(SPSSODescriptor.class));
        final SPSSODescriptor spDescriptor = (SPSSODescriptor) descriptor.getRoleDescriptors().get(0);

        assertThat(spDescriptor.getAttributeConsumingServices(), iterableWithSize(1));
        final List<RequestedAttribute> attributes = spDescriptor.getAttributeConsumingServices().get(0).getRequestedAttributes();
        assertThat(attributes, iterableWithSize(4));

        assertThat(attributes.get(0).getFriendlyName(), equalTo("mail"));
        assertThat(attributes.get(0).getName(), equalTo("urn:oid:0.9.2342.19200300.100.1.3"));

        assertThat(attributes.get(1).getFriendlyName(), equalTo("groups"));
        assertThat(attributes.get(1).getName(), equalTo("urn:oid:1.3.6.1.4.1.5923.1.5.1.1"));

        assertThat(attributes.get(2).getFriendlyName(), equalTo("displayName"));
        assertThat(attributes.get(2).getName(), equalTo("urn:oid:2.16.840.1.113730.3.1.241"));

        assertThat(attributes.get(3).getFriendlyName(), equalTo("uid"));
        assertThat(attributes.get(3).getName(), equalTo("urn:oid:0.9.2342.19200300.100.1.1"));
    }

    public void testHandleAttributesInBatchMode() throws Exception {
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.saml1.order", 1)
                .put(RealmSettings.PREFIX + "saml.saml1.type", "saml")
                .put(RealmSettings.PREFIX + "saml.saml1.sp.entity_id", "https://saml.example.com/")
                .put(RealmSettings.PREFIX + "saml.saml1.sp.acs", "https://saml.example.com/")
                .put(RealmSettings.PREFIX + "saml.saml1.attributes.principal", "urn:oid:0.9.2342.19200300.100.1.1")
                .build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[] {
                "-attribute", "urn:oid:0.9.2342.19200300.100.1.3",
                "-batch"
        });

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);

        assertThat(descriptor, notNullValue());
        assertThat(descriptor.getEntityID(), equalTo("https://saml.example.com/"));

        assertThat(descriptor.getRoleDescriptors(), iterableWithSize(1));
        assertThat(descriptor.getRoleDescriptors().get(0), instanceOf(SPSSODescriptor.class));
        final SPSSODescriptor spDescriptor = (SPSSODescriptor) descriptor.getRoleDescriptors().get(0);

        assertThat(spDescriptor.getAttributeConsumingServices(), iterableWithSize(1));
        final List<RequestedAttribute> attributes = spDescriptor.getAttributeConsumingServices().get(0).getRequestedAttributes();
        assertThat(attributes, iterableWithSize(2));

        assertThat(attributes.get(0).getFriendlyName(), nullValue());
        assertThat(attributes.get(0).getName(), equalTo("urn:oid:0.9.2342.19200300.100.1.3"));

        assertThat(attributes.get(1).getFriendlyName(), equalTo("principal"));
        assertThat(attributes.get(1).getName(), equalTo("urn:oid:0.9.2342.19200300.100.1.1"));
    }

    public void testSigningMetadataWithPfx() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PKCS12 keystores are not usable", inFipsJvm());
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        final Path certPath = getDataPath("saml.crt");
        final Path keyPath = getDataPath("saml.key");
        final Path p12Path = getDataPath("saml.p12");
        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[]{
                "-signing-bundle", p12Path.toString()
        });

        final boolean useSigningCredentials = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.my_saml.type", "saml")
                .put(RealmSettings.PREFIX + "saml.my_saml.order", 1)
                .put(RealmSettings.PREFIX + "saml.my_saml.idp.entity_id", "https://okta.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.entity_id", "https://kibana.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.acs", "https://kibana.my.corp/saml/login")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.logout", "https://kibana.my.corp/saml/logout")
                .put(RealmSettings.PREFIX + "saml.my_saml.attributes.principal", "urn:oid:0.9.2342.19200300.100.1.1");
        if (useSigningCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.signing.certificate", certPath.toString())
                    .put(RealmSettings.PREFIX + "saml.my_saml.signing.key", keyPath.toString());
        }
        final Settings settings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        // What is the friendly name for "principal" attribute "urn:oid:0.9.2342.19200300.100.1.1" [default: principal]
        terminal.addTextInput("");
        terminal.addSecretInput("");

        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);
        command.possiblySignDescriptor(terminal, options, descriptor, env);
        assertThat(descriptor, notNullValue());
        // Verify generated signature
        assertThat(descriptor.getSignature(), notNullValue());
        assertThat(validateSignature(descriptor.getSignature()), equalTo(true));
        // Make sure that Signing didn't mangle the XML at all and we can still read metadata
        assertThat(descriptor.getEntityID(), equalTo("https://kibana.my.corp/"));

        assertThat(descriptor.getRoleDescriptors(), iterableWithSize(1));
        assertThat(descriptor.getRoleDescriptors().get(0), instanceOf(SPSSODescriptor.class));
        final SPSSODescriptor spDescriptor = (SPSSODescriptor) descriptor.getRoleDescriptors().get(0);

        assertThat(spDescriptor.getAssertionConsumerServices(), iterableWithSize(1));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getLocation(), equalTo("https://kibana.my.corp/saml/login"));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).isDefault(), equalTo(true));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getIndex(), equalTo(1));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getBinding(), equalTo(SAMLConstants.SAML2_POST_BINDING_URI));

        final RequestedAttribute uidAttribute = spDescriptor.getAttributeConsumingServices().get(0).getRequestedAttributes().get(0);
        assertThat(uidAttribute.getName(), equalTo("urn:oid:0.9.2342.19200300.100.1.1"));
        assertThat(uidAttribute.getFriendlyName(), equalTo("principal"));

        assertThat(spDescriptor.isAuthnRequestsSigned(), equalTo(useSigningCredentials));
        assertThat(spDescriptor.getWantAssertionsSigned(), equalTo(true));
    }

    public void testSigningMetadataWithPasswordProtectedPfx() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PKCS12 keystores are not usable", inFipsJvm());
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        final Path certPath = getDataPath("saml.crt");
        final Path keyPath = getDataPath("saml.key");
        final Path p12Path = getDataPath("saml_with_password.p12");
        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[]{
                "-signing-bundle", p12Path.toString(),
                "-signing-key-password", "saml"
        });

        final boolean useSigningCredentials = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.my_saml.type", "saml")
                .put(RealmSettings.PREFIX + "saml.my_saml.order", 1)
                .put(RealmSettings.PREFIX + "saml.my_saml.idp.entity_id", "https://okta.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.entity_id", "https://kibana.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.acs", "https://kibana.my.corp/saml/login")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.logout", "https://kibana.my.corp/saml/logout");
        if (useSigningCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.signing.certificate", certPath.toString())
                    .put(RealmSettings.PREFIX + "saml.my_saml.signing.key", keyPath.toString());
        }
        final Settings settings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);
        command.possiblySignDescriptor(terminal, options, descriptor, env);
        assertThat(descriptor, notNullValue());
        // Verify generated signature
        assertThat(descriptor.getSignature(), notNullValue());
        assertThat(validateSignature(descriptor.getSignature()), equalTo(true));
    }

    public void testErrorSigningMetadataWithWrongPassword() throws Exception {
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        final Path certPath = getDataPath("saml.crt");
        final Path keyPath = getDataPath("saml.key");
        final Path signingKeyPath = getDataPath("saml_with_password.key");
        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> keyStore);
        final OptionSet options = command.getParser().parse(new String[]{
            "-signing-cert", certPath.toString(),
            "-signing-key", signingKeyPath.toString(),
            "-signing-key-password", "wrongpassword"

        });

        final boolean useSigningCredentials = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.my_saml.type", "saml")
                .put(RealmSettings.PREFIX + "saml.my_saml.order", 1)
                .put(RealmSettings.PREFIX + "saml.my_saml.idp.entity_id", "https://okta.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.entity_id", "https://kibana.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.acs", "https://kibana.my.corp/saml/login")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.logout", "https://kibana.my.corp/saml/logout");
        if (useSigningCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.signing.certificate", certPath.toString())
                    .put(RealmSettings.PREFIX + "saml.my_saml.signing.key", keyPath.toString());
        }
        final Settings settings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);
        final UserException userException = expectThrows(UserException.class, () -> command.possiblySignDescriptor(terminal, options,
                descriptor, env));
        assertThat(userException.getMessage(), containsString("Unable to create metadata document"));
        assertThat(terminal.getErrorOutput(), containsString("cannot load PEM private key from ["));
    }

    public void testSigningMetadataWithPem() throws Exception {
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        //Use this keypair for signing the metadata also
        final Path certPath = getDataPath("saml.crt");
        final Path keyPath = getDataPath("saml.key");

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> keyStore);
        final OptionSet options = command.getParser().parse(new String[]{
                "-signing-cert", certPath.toString(),
                "-signing-key", keyPath.toString()
        });

        final boolean useSigningCredentials = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.my_saml.type", "saml")
                .put(RealmSettings.PREFIX + "saml.my_saml.order", 1)
                .put(RealmSettings.PREFIX + "saml.my_saml.idp.entity_id", "https://okta.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.entity_id", "https://kibana.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.acs", "https://kibana.my.corp/saml/login")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.logout", "https://kibana.my.corp/saml/logout");
        if (useSigningCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.signing.certificate", certPath.toString())
                    .put(RealmSettings.PREFIX + "saml.my_saml.signing.key", keyPath.toString());
        }
        final Settings settings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);
        command.possiblySignDescriptor(terminal, options, descriptor, env);
        assertThat(descriptor, notNullValue());
        // Verify generated signature
        assertThat(descriptor.getSignature(), notNullValue());
        assertThat(validateSignature(descriptor.getSignature()), equalTo(true));
    }

    public void testSigningMetadataWithPasswordProtectedPem() throws Exception {
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        //Use same keypair for signing the metadata
        final Path signingKeyPath = getDataPath("saml_with_password.key");

        final Path certPath = getDataPath("saml.crt");
        final Path keyPath = getDataPath("saml.key");

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[]{
                "-signing-cert", certPath.toString(),
                "-signing-key", signingKeyPath.toString(),
            "-signing-key-password", "saml"

        });

        final boolean useSigningCredentials = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.my_saml.type", "saml")
                .put(RealmSettings.PREFIX + "saml.my_saml.order", 1)
                .put(RealmSettings.PREFIX + "saml.my_saml.idp.entity_id", "https://okta.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.entity_id", "https://kibana.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.acs", "https://kibana.my.corp/saml/login")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.logout", "https://kibana.my.corp/saml/logout");
        if (useSigningCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.signing.certificate", certPath.toString())
                    .put(RealmSettings.PREFIX + "saml.my_saml.signing.key", keyPath.toString());
        }
        final Settings settings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);
        command.possiblySignDescriptor(terminal, options, descriptor, env);
        assertThat(descriptor, notNullValue());
        // Verify generated signature
        assertThat(descriptor.getSignature(), notNullValue());
        assertThat(validateSignature(descriptor.getSignature()), equalTo(true));
    }

    public void testSigningMetadataWithPasswordProtectedPemInTerminal() throws Exception {
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        //Use same keypair for signing the metadata
        final Path signingKeyPath = getDataPath("saml_with_password.key");

        final Path certPath = getDataPath("saml.crt");
        final Path keyPath = getDataPath("saml.key");

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[]{
                "-signing-cert", certPath.toString(),
                "-signing-key", signingKeyPath.toString()

        });

        final boolean useSigningCredentials = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml.my_saml.type", "saml")
                .put(RealmSettings.PREFIX + "saml.my_saml.order", 1)
                .put(RealmSettings.PREFIX + "saml.my_saml.idp.entity_id", "https://okta.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.entity_id", "https://kibana.my.corp/")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.acs", "https://kibana.my.corp/saml/login")
                .put(RealmSettings.PREFIX + "saml.my_saml.sp.logout", "https://kibana.my.corp/saml/logout");
        if (useSigningCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.signing.certificate", certPath.toString())
                    .put(RealmSettings.PREFIX + "saml.my_saml.signing.key", keyPath.toString());
        }
        final Settings settings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        terminal.addSecretInput("saml");

        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);
        command.possiblySignDescriptor(terminal, options, descriptor, env);
        assertThat(descriptor, notNullValue());
        // Verify generated signature
        assertThat(descriptor.getSignature(), notNullValue());
        assertThat(validateSignature(descriptor.getSignature()), equalTo(true));
    }

    public void testDefaultOptionsWithSigningAndMultipleEncryptionKeys() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PKCS12 keystores are not usable", inFipsJvm());
        final KeyStoreWrapper usedKeyStore = randomFrom(keyStore, passwordProtectedKeystore);
        final Path dir = createTempDir();

        final Path ksEncryptionFile = dir.resolve("saml-encryption.p12");
        final Tuple<java.security.cert.X509Certificate, PrivateKey> certEncKeyPair1 = readKeyPair("RSA_2048");
        final Tuple<java.security.cert.X509Certificate, PrivateKey> certEncKeyPair2 = readKeyPair("RSA_4096");
        final KeyStore ksEncrypt = KeyStore.getInstance("PKCS12");
        ksEncrypt.load(null);
        ksEncrypt.setKeyEntry(getAliasName(certEncKeyPair1), certEncKeyPair1.v2(), "key-password".toCharArray(),
                new Certificate[] { certEncKeyPair1.v1() });
        ksEncrypt.setKeyEntry(getAliasName(certEncKeyPair2), certEncKeyPair2.v2(), "key-password".toCharArray(),
                new Certificate[] { certEncKeyPair2.v1() });
        try (OutputStream out = Files.newOutputStream(ksEncryptionFile)) {
            ksEncrypt.store(out, "ks-password".toCharArray());
        }

        final Path ksSigningFile = dir.resolve("saml-signing.p12");
        final Tuple<java.security.cert.X509Certificate, PrivateKey> certKeyPairSign = readRandomKeyPair("RSA");
        final KeyStore ksSign = KeyStore.getInstance("PKCS12");
        ksSign.load(null);
        ksSign.setKeyEntry(getAliasName(certKeyPairSign), certKeyPairSign.v2(), "key-password".toCharArray(),
                new Certificate[] { certKeyPairSign.v1() });
        try (OutputStream out = Files.newOutputStream(ksSigningFile)) {
            ksSign.store(out, "ks-password".toCharArray());
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(RealmSettings.PREFIX + "saml.my_saml.signing.keystore.secure_password", "ks-password");
        secureSettings.setString(RealmSettings.PREFIX + "saml.my_saml.signing.keystore.secure_key_password", "key-password");
        secureSettings.setString(RealmSettings.PREFIX + "saml.my_saml.encryption.keystore.secure_password", "ks-password");
        secureSettings.setString(RealmSettings.PREFIX + "saml.my_saml.encryption.keystore.secure_key_password", "key-password");

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> usedKeyStore);
        final OptionSet options = command.getParser().parse(new String[0]);

        final boolean useSigningCredentials = randomBoolean();
        final boolean useEncryptionCredentials = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder().put("path.home", dir)
            .put(RealmSettings.PREFIX + "saml.my_saml.type", "saml")
            .put(RealmSettings.PREFIX + "saml.my_saml.order", 1)
            .put(RealmSettings.PREFIX + "saml.my_saml.idp.entity_id", "https://okta.my.corp/")
            .put(RealmSettings.PREFIX + "saml.my_saml.sp.entity_id", "https://kibana.my.corp/")
            .put(RealmSettings.PREFIX + "saml.my_saml.sp.acs", "https://kibana.my.corp/saml/login")
            .put(RealmSettings.PREFIX + "saml.my_saml.sp.logout", "https://kibana.my.corp/saml/logout")
            .put(RealmSettings.PREFIX + "saml.my_saml.attributes.principal", "urn:oid:0.9.2342.19200300.100.1.1");
        settingsBuilder.setSecureSettings(secureSettings);
        if (useSigningCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.signing.keystore.path", ksSigningFile.toString());
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.signing.keystore.type", "PKCS12");
        }
        if (useEncryptionCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.encryption.keystore.path", ksEncryptionFile.toString());
            settingsBuilder.put(RealmSettings.PREFIX + "saml.my_saml.encryption.keystore.type", "PKCS12");
        }
        final Settings settings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = getTerminalPossiblyWithPassword(usedKeyStore);
        // What is the friendly name for "principal" attribute
        // "urn:oid:0.9.2342.19200300.100.1.1" [default: principal]
        terminal.addTextInput("");

        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);

        assertThat(descriptor, notNullValue());
        assertThat(descriptor.getEntityID(), equalTo("https://kibana.my.corp/"));

        assertThat(descriptor.getRoleDescriptors(), iterableWithSize(1));
        assertThat(descriptor.getRoleDescriptors().get(0), instanceOf(SPSSODescriptor.class));
        final SPSSODescriptor spDescriptor = (SPSSODescriptor) descriptor.getRoleDescriptors().get(0);

        assertThat(spDescriptor.getAssertionConsumerServices(), iterableWithSize(1));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getLocation(), equalTo("https://kibana.my.corp/saml/login"));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).isDefault(), equalTo(true));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getIndex(), equalTo(1));
        assertThat(spDescriptor.getAssertionConsumerServices().get(0).getBinding(), equalTo(SAMLConstants.SAML2_POST_BINDING_URI));

        assertThat(spDescriptor.getAttributeConsumingServices(), iterableWithSize(1));
        assertThat(spDescriptor.getAttributeConsumingServices().get(0).isDefault(), equalTo(true));
        assertThat(spDescriptor.getAttributeConsumingServices().get(0).getIndex(), equalTo(1));
        assertThat(spDescriptor.getAttributeConsumingServices().get(0).getRequestedAttributes(), iterableWithSize(1));
        final RequestedAttribute uidAttribute = spDescriptor.getAttributeConsumingServices().get(0).getRequestedAttributes().get(0);
        assertThat(uidAttribute.getName(), equalTo("urn:oid:0.9.2342.19200300.100.1.1"));
        assertThat(uidAttribute.getFriendlyName(), equalTo("principal"));

        assertThat(spDescriptor.getSingleLogoutServices(), iterableWithSize(1));
        assertThat(spDescriptor.getSingleLogoutServices().get(0).getLocation(), equalTo("https://kibana.my.corp/saml/logout"));
        assertThat(spDescriptor.getSingleLogoutServices().get(0).getBinding(), equalTo(SAMLConstants.SAML2_REDIRECT_BINDING_URI));

        assertThat(spDescriptor.isAuthnRequestsSigned(), equalTo(useSigningCredentials));
        assertThat(spDescriptor.getWantAssertionsSigned(), equalTo(true));

        int expectedKeyDescriptorSize = (useSigningCredentials) ? 1 : 0;
        expectedKeyDescriptorSize = (useEncryptionCredentials) ? expectedKeyDescriptorSize + 2 : expectedKeyDescriptorSize;

        assertThat(spDescriptor.getKeyDescriptors(), iterableWithSize(expectedKeyDescriptorSize));
        if (expectedKeyDescriptorSize > 0) {
            final Set<java.security.cert.X509Certificate> encryptionCertificatesToMatch = new HashSet<>();
            if (useEncryptionCredentials) {
                encryptionCertificatesToMatch.add(certEncKeyPair1.v1());
                encryptionCertificatesToMatch.add(certEncKeyPair2.v1());
            }
            spDescriptor.getKeyDescriptors().stream().forEach((keyDesc) -> {
                UsageType usageType = keyDesc.getUse();
                final List<X509Data> x509 = keyDesc.getKeyInfo().getX509Datas();
                assertThat(x509, iterableWithSize(1));
                assertThat(x509.get(0).getX509Certificates(), iterableWithSize(1));
                final X509Certificate xmlCert = x509.get(0).getX509Certificates().get(0);
                final java.security.cert.X509Certificate javaCert;
                try {
                    // Verify that OpenSAML things the XML representation is the same as our input
                    javaCert = KeyInfoSupport.getCertificate(xmlCert);
                } catch (CertificateException ce) {
                    throw ExceptionsHelper.convertToRuntime(ce);
                }
                if (usageType == UsageType.SIGNING) {
                    assertTrue("Found UsageType as SIGNING in SP metadata when not testing for signing credentials", useSigningCredentials);
                    assertEquals("Signing Certificate from SP metadata does not match", certKeyPairSign.v1(), javaCert);
                } else if (usageType == UsageType.ENCRYPTION) {
                    assertTrue(useEncryptionCredentials);
                    assertTrue("Encryption Certificate was not found in encryption certificates",
                            encryptionCertificatesToMatch.remove(javaCert));
                } else {
                    fail("Usage type should have been either SIGNING or ENCRYPTION");
                }
            });
            if (useEncryptionCredentials) {
                assertTrue("Did not find all encryption certificates in exported SP metadata", encryptionCertificatesToMatch.isEmpty());
            }
        }
    }

    public void testWrongKeystorePassword() {
        final Path certPath = getDataPath("saml.crt");
        final Path keyPath = getDataPath("saml.key");

        final SamlMetadataCommand command = new SamlMetadataCommand((e) -> passwordProtectedKeystore);
        final OptionSet options = command.getParser().parse(new String[]{
            "-signing-cert", certPath.toString(),
            "-signing-key", keyPath.toString()
        });
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = new MockTerminal();
        terminal.addSecretInput("wrong-password");

        UserException e = expectThrows(UserException.class, () -> {
            command.buildEntityDescriptor(terminal, options, env);
        });
        assertThat(e.getMessage(), CoreMatchers.containsString("Provided keystore password was incorrect"));
    }

    private String getAliasName(final Tuple<java.security.cert.X509Certificate, PrivateKey> certKeyPair) {
        // Keys are pre-generated with the same name, so add the serial no to the alias so that keystore entries won't be overwritten
        return certKeyPair.v1().getSubjectX500Principal().getName().toLowerCase(Locale.US) + "-"+
            certKeyPair.v1().getSerialNumber()+"-alias";
    }

    private boolean validateSignature(Signature signature) {
        try {
            java.security.cert.X509Certificate certificate = CertParsingUtils.readX509Certificate(getDataPath("saml.crt"));
            PrivateKey key = org.elasticsearch.common.ssl.PemUtils.readPrivateKey(getDataPath("saml.key"), ""::toCharArray);
            Credential verificationCredential = new BasicX509Credential(certificate, key);
            SAMLSignatureProfileValidator profileValidator = new SAMLSignatureProfileValidator();
            profileValidator.validate(signature);
            SignatureValidator.validate(signature, verificationCredential);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private MockTerminal getTerminalPossiblyWithPassword(KeyStoreWrapper keyStore) {
        final MockTerminal terminal = new MockTerminal();
        if (keyStore.hasPassword()) {
            terminal.addSecretInput("keystore-password");
        }
        return terminal;
    }
}
