/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import joptsimple.OptionSet;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.ssl.CertUtils;
import org.junit.Before;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.RequestedAttribute;
import org.opensaml.saml.saml2.metadata.SPSSODescriptor;
import org.opensaml.security.credential.UsageType;
import org.opensaml.xmlsec.keyinfo.KeyInfoSupport;
import org.opensaml.xmlsec.signature.X509Certificate;
import org.opensaml.xmlsec.signature.X509Data;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class SamlMetadataCommandTests extends SamlTestCase {

    @Before
    public void setup() throws Exception {
        SamlUtils.initialize(logger);
    }

    public void testDefaultOptions() throws Exception {
        final Path certPath = getDataPath("saml.crt");
        final Path keyPath = getDataPath("saml.key");

        final SamlMetadataCommand command = new SamlMetadataCommand();
        final OptionSet options = command.getParser().parse(new String[0]);

        final boolean useSigningCredentials = randomBoolean();
        final Settings.Builder settingsBuilder = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "my_saml.type", "saml")
                .put(RealmSettings.PREFIX + "my_saml.order", 1)
                .put(RealmSettings.PREFIX + "my_saml.idp.entity_id", "https://okta.my.corp/")
                .put(RealmSettings.PREFIX + "my_saml.sp.entity_id", "https://kibana.my.corp/")
                .put(RealmSettings.PREFIX + "my_saml.sp.acs", "https://kibana.my.corp/saml/login")
                .put(RealmSettings.PREFIX + "my_saml.sp.logout", "https://kibana.my.corp/saml/logout")
                .put(RealmSettings.PREFIX + "my_saml.attributes.principal", "urn:oid:0.9.2342.19200300.100.1.1");
        if (useSigningCredentials) {
            settingsBuilder.put(RealmSettings.PREFIX + "my_saml.signing.certificate", certPath.toString())
                    .put(RealmSettings.PREFIX + "my_saml.signing.key", keyPath.toString());
        }
        final Settings settings = settingsBuilder.build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final MockTerminal terminal = new MockTerminal();

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
        assertThat(spDescriptor.getAttributeConsumingServices().get(0).getRequestAttributes(), iterableWithSize(1));
        final RequestedAttribute uidAttribute = spDescriptor.getAttributeConsumingServices().get(0).getRequestAttributes().get(0);
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

            // Verify that OpenSAML things the XML representation is the same as our input
            final java.security.cert.X509Certificate javaCert = KeyInfoSupport.getCertificate(xmlCert);
            assertThat(CertUtils.readCertificates(Collections.singletonList(certPath)), arrayContaining(javaCert));
        } else {
            assertThat(spDescriptor.getKeyDescriptors(), iterableWithSize(0));
        }
    }

    public void testFailIfMultipleRealmsExist() throws Exception {
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml_a.type", "saml")
                .put(RealmSettings.PREFIX + "saml_a.sp.entity_id", "https://saml.a/")
                .put(RealmSettings.PREFIX + "saml_a.sp.acs", "https://saml.a/")
                .put(RealmSettings.PREFIX + "saml_b.type", "saml")
                .put(RealmSettings.PREFIX + "saml_b.sp.entity_id", "https://saml.b/")
                .put(RealmSettings.PREFIX + "saml_b.sp.acs", "https://saml.b/")
                .build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final SamlMetadataCommand command = new SamlMetadataCommand();
        final OptionSet options = command.getParser().parse(new String[0]);

        final MockTerminal terminal = new MockTerminal();

        final UserException userException = expectThrows(UserException.class, () -> command.buildEntityDescriptor(terminal, options, env));
        assertThat(userException.getMessage(), containsString("multiple SAML realms"));
        assertThat(terminal.getOutput(), containsString("saml_a"));
        assertThat(terminal.getOutput(), containsString("saml_b"));
        assertThat(terminal.getOutput(), containsString("Use the -realm option"));
    }

    public void testSpecifyRealmNameAsParameter() throws Exception {
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml_a.type", "saml")
                .put(RealmSettings.PREFIX + "saml_a.sp.entity_id", "https://saml.a/")
                .put(RealmSettings.PREFIX + "saml_a.sp.acs", "https://saml.a/acs")
                .put(RealmSettings.PREFIX + "saml_b.type", "saml")
                .put(RealmSettings.PREFIX + "saml_b.sp.entity_id", "https://saml.b/")
                .put(RealmSettings.PREFIX + "saml_b.sp.acs", "https://saml.b/acs")
                .build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final SamlMetadataCommand command = new SamlMetadataCommand();
        final OptionSet options = command.getParser().parse(new String[] {
                "-realm", "saml_b"
        });

        final MockTerminal terminal = new MockTerminal();
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
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml1.type", "saml")
                .put(RealmSettings.PREFIX + "saml1.sp.entity_id", "https://saml.example.com/")
                .put(RealmSettings.PREFIX + "saml1.sp.acs", "https://saml.example.com/")
                .put(RealmSettings.PREFIX + "saml1.attributes.principal", "urn:oid:0.9.2342.19200300.100.1.1")
                .put(RealmSettings.PREFIX + "saml1.attributes.name", "displayName")
                .build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final SamlMetadataCommand command = new SamlMetadataCommand();
        final OptionSet options = command.getParser().parse(new String[] {
                "-attribute", "urn:oid:0.9.2342.19200300.100.1.3",
                "-attribute", "groups"
        });

        final MockTerminal terminal = new MockTerminal();

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
        final List<RequestedAttribute> attributes = spDescriptor.getAttributeConsumingServices().get(0).getRequestAttributes();
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
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.PREFIX + "saml1.type", "saml")
                .put(RealmSettings.PREFIX + "saml1.sp.entity_id", "https://saml.example.com/")
                .put(RealmSettings.PREFIX + "saml1.sp.acs", "https://saml.example.com/")
                .put(RealmSettings.PREFIX + "saml1.attributes.principal", "urn:oid:0.9.2342.19200300.100.1.1")
                .build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        final SamlMetadataCommand command = new SamlMetadataCommand();
        final OptionSet options = command.getParser().parse(new String[] {
                "-attribute", "urn:oid:0.9.2342.19200300.100.1.3",
                "-batch"
        });

        final MockTerminal terminal = new MockTerminal();
        final EntityDescriptor descriptor = command.buildEntityDescriptor(terminal, options, env);

        assertThat(descriptor, notNullValue());
        assertThat(descriptor.getEntityID(), equalTo("https://saml.example.com/"));

        assertThat(descriptor.getRoleDescriptors(), iterableWithSize(1));
        assertThat(descriptor.getRoleDescriptors().get(0), instanceOf(SPSSODescriptor.class));
        final SPSSODescriptor spDescriptor = (SPSSODescriptor) descriptor.getRoleDescriptors().get(0);

        assertThat(spDescriptor.getAttributeConsumingServices(), iterableWithSize(1));
        final List<RequestedAttribute> attributes = spDescriptor.getAttributeConsumingServices().get(0).getRequestAttributes();
        assertThat(attributes, iterableWithSize(2));

        assertThat(attributes.get(0).getFriendlyName(), nullValue());
        assertThat(attributes.get(0).getName(), equalTo("urn:oid:0.9.2342.19200300.100.1.3"));

        assertThat(attributes.get(1).getFriendlyName(), equalTo("principal"));
        assertThat(attributes.get(1).getName(), equalTo("urn:oid:0.9.2342.19200300.100.1.1"));
    }
}