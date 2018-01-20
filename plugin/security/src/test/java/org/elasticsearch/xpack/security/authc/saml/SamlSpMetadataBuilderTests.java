/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Locale;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ssl.CertUtils;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorMarshaller;
import org.w3c.dom.Element;

public class SamlSpMetadataBuilderTests extends SamlTestCase {

    private X509Certificate certificate;

    @Before
    public void setup() throws Exception {
        SamlUtils.initialize(logger);
        final Path certPath = getDataPath("saml.crt");
        final Certificate[] certs = CertUtils.readCertificates(Collections.singletonList(certPath));
        if (certs.length != 1) {
            fail("Expected exactly 1 certificate in " + certPath);
        }
        if (certs[0] instanceof X509Certificate) {
            this.certificate = (X509Certificate) certs[0];
        } else {
            fail("Expected exactly X509Certificate, but was " + certs[0].getClass());
        }
    }

    public void testBuildMinimalistMetadata() throws Exception {
        final EntityDescriptor descriptor = new SamlSpMetadataBuilder(Locale.getDefault(), "https://my.sp.example.net/")
                .assertionConsumerServiceUrl("https://my.sp.example.net/saml/acs/post")
                .build();

        final Element element = new EntityDescriptorMarshaller().marshall(descriptor);
        final String xml = SamlUtils.toString(element);
        assertThat(xml, Matchers.equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"https://my.sp.example.net/\">" +
                "<md:SPSSODescriptor AuthnRequestsSigned=\"false\" WantAssertionsSigned=\"true\"" +
                " protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">" +
                "<md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:transient</md:NameIDFormat>" +
                "<md:AssertionConsumerService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\"" +
                " Location=\"https://my.sp.example.net/saml/acs/post\" index=\"1\" isDefault=\"true\"/>" +
                "</md:SPSSODescriptor>" +
                "</md:EntityDescriptor>"
        ));
        assertValidXml(xml);
    }

    public void testBuildFullMetadata() throws Exception {
        final EntityDescriptor descriptor = new SamlSpMetadataBuilder(Locale.US, "https://kibana.apps.hydra/")
                .serviceName("Hydra Kibana")
                .nameIdFormat(NameID.PERSISTENT)
                .withAttribute("uid", "urn:oid:0.9.2342.19200300.100.1.1")
                .withAttribute("mail", "urn:oid:0.9.2342.19200300.100.1.3")
                .withAttribute("groups", "urn:oid:1.3.6.1.4.1.5923.1.5.1.1")
                .withAttribute(null, "urn:oid:2.16.840.1.113730.3.1.241")
                .withAttribute(null, "urn:oid:1.3.6.1.4.1.5923.1.1.1.6")
                .assertionConsumerServiceUrl("https://kibana.apps.hydra/saml/acs")
                .singleLogoutServiceUrl("https://kibana.apps.hydra/saml/logout")
                .authnRequestsSigned(true)
                .signingCertificate(certificate)
                .encryptionCertificate(certificate)
                .organization("Hydra", "Hydra", "https://hail.hydra/")
                .withContact("administrative", "Wolfgang", "von Strucker", "baron.strucker@supreme.hydra")
                .withContact("technical", "Paul", "Ebersol", "pne@tech.hydra")
                .build();

        final Element element = new EntityDescriptorMarshaller().marshall(descriptor);
        final String xml = SamlUtils.toString(element);
        assertThat(xml, Matchers.equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"https://kibana.apps.hydra/\">" +
                "<md:SPSSODescriptor AuthnRequestsSigned=\"true\" WantAssertionsSigned=\"true\"" +
                " protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">" +
                "<md:KeyDescriptor>" +
                "<ds:KeyInfo xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:X509Data><ds:X509Certificate>" +
                "MIIDWDCCAkCgAwIBAgIVANRTZaFrK+Pz19O8TZsb3HSJmAWpMA0GCSqGSIb3DQEBCwUAMB0xGzAZ\n" +
                "BgNVBAMTEkVsYXN0aWNzZWFyY2gtU0FNTDAeFw0xNzExMjkwMjQ3MjZaFw0yMDExMjgwMjQ3MjZa\n" +
                "MB0xGzAZBgNVBAMTEkVsYXN0aWNzZWFyY2gtU0FNTDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC\n" +
                "AQoCggEBALHTuPGOieCbD2mZUdYrdH4ofo7qFze6rQUROCLKqf69uBuwvraNWOcwxHUTKVlLMV3d\n" +
                "dKzYo+yfC44AMXrrV+79xVWsTCNHu9sxQzcDwiEx2OtOOX9MAk6tJQ3svNrMPNXWh8ftwmmY9XdF\n" +
                "ZwMYUdo6FPjSQj5uQTDmGWRgF08f7VRlk6N92d/fzn9DlDm+TFuaOr17OTSR4B6RTrNwKC29AmXQ\n" +
                "TwCijCObjLqyMEqP20dZCQeVf2qw8JKUHhW4r6mCLzqmeR+kRTqiHMSWxJddzxDGw6X7fOS7iuzB\n" +
                "0+TnsKwgu8nYrEXds9MkGf1Yco7WsM43g+Es+LhNHP+es70CAwEAAaOBjjCBizAdBgNVHQ4EFgQU\n" +
                "ILqVKGhIi8p5Xffsow/IKFLhRbIwWQYDVR0jBFIwUIAUILqVKGhIi8p5Xffsow/IKFLhRbKhIaQf\n" +
                "MB0xGzAZBgNVBAMTEkVsYXN0aWNzZWFyY2gtU0FNTIIVANRTZaFrK+Pz19O8TZsb3HSJmAWpMA8G\n" +
                "A1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAGhl4V9mp4SWSV2E3HAJ1PX+Vmp6k27K\n" +
                "d0tkOk1B9fyA13QB30teyiL7RR0vSHRyWFY8rQH1mHD366GKRWLITRG/QPULamGdYXX4h0pFj5ld\n" +
                "aubLxM/O9vEAxOgmo/lsdkeIq9tLBqY06r/5A/Mcgo63KGi00AFYBoyvqfOu6nRLPnQr+rKVfdNO\n" +
                "pWeIiFY1i2XTNZ3CZjNPSTwiQMUzrCxKXB9lL0vF6QL2Gj2iBhzNfXi88wf7xaR6XKY1wNuv3HLP\n" +
                "sL7n+PWby7LRX188dyS1dmKfQcrKL65OssBA5NC8CAYyBiygBmWN+5kVJM5fSb0SwPSoVWrNyz+8\n" +
                "IUldQE8=" +
                "</ds:X509Certificate></ds:X509Data></ds:KeyInfo>" +
                "</md:KeyDescriptor>" +
                "<md:SingleLogoutService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\"" +
                " Location=\"https://kibana.apps.hydra/saml/logout\"/>" +
                "<md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:persistent</md:NameIDFormat>" +
                "<md:AssertionConsumerService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\"" +
                " Location=\"https://kibana.apps.hydra/saml/acs\" index=\"1\" isDefault=\"true\"/>" +
                "<md:AttributeConsumingService index=\"1\" isDefault=\"true\">" +
                "<md:ServiceName xml:lang=\"en-US\">Hydra Kibana</md:ServiceName>" +
                "<md:RequestedAttribute FriendlyName=\"uid\" Name=\"urn:oid:0.9.2342.19200300.100.1.1\"" +
                " NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>" +
                "<md:RequestedAttribute FriendlyName=\"mail\" Name=\"urn:oid:0.9.2342.19200300.100.1.3\"" +
                " NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>" +
                "<md:RequestedAttribute FriendlyName=\"groups\" Name=\"urn:oid:1.3.6.1.4.1.5923.1.5.1.1\"" +
                " NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>" +
                "<md:RequestedAttribute Name=\"urn:oid:2.16.840.1.113730.3.1.241\"" +
                " NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>" +
                "<md:RequestedAttribute Name=\"urn:oid:1.3.6.1.4.1.5923.1.1.1.6\"" +
                " NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>" +
                "</md:AttributeConsumingService>" +
                "</md:SPSSODescriptor>" +
                "<md:Organization>" +
                "<md:OrganizationName xml:lang=\"en-US\">Hydra</md:OrganizationName>" +
                "<md:OrganizationDisplayName xml:lang=\"en-US\">Hydra</md:OrganizationDisplayName>" +
                "<md:OrganizationURL xml:lang=\"en-US\">https://hail.hydra/</md:OrganizationURL>" +
                "</md:Organization>" +
                "<md:ContactPerson contactType=\"administrative\">" +
                "<md:GivenName>Wolfgang</md:GivenName>" +
                "<md:SurName>von Strucker</md:SurName>" +
                "<md:EmailAddress>baron.strucker@supreme.hydra</md:EmailAddress>" +
                "</md:ContactPerson>" +
                "<md:ContactPerson contactType=\"technical\">" +
                "<md:GivenName>Paul</md:GivenName>" +
                "<md:SurName>Ebersol</md:SurName>" +
                "<md:EmailAddress>pne@tech.hydra</md:EmailAddress>" +
                "</md:ContactPerson>" +
                "</md:EntityDescriptor>"
        ));
        assertValidXml(xml);
    }

    public void testAssertionConsumerServiceIsRequired() {
        final SamlSpMetadataBuilder builder = new SamlSpMetadataBuilder(Locale.US, "https://kibana.apps.hydra/");
        final IllegalStateException exception = expectThrows(IllegalStateException.class, builder::build);
        assertThat(exception.getMessage(), Matchers.containsString("AssertionConsumerService URL"));
    }

    public void testAttributeNameIsRequired() {
        final SamlSpMetadataBuilder builder = new SamlSpMetadataBuilder(Locale.US, "https://kibana.example.net/");
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> builder.withAttribute("uid", ""));
        assertThat(exception.getMessage(), Matchers.containsString("Attribute name"));
    }

    private void assertValidXml(String xml) throws Exception {
        SamlUtils.validate(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), SamlMetadataCommand.METADATA_SCHEMA);
    }
}