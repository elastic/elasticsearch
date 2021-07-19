/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.util.NamedFormatter;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorMarshaller;
import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class SamlSpMetadataBuilderTests extends SamlTestCase {

    private X509Certificate certificate;

    // 1st is for signing, followed by 2 for encryption
    private X509Certificate[] threeCertificates;

    @Before
    public void setup() throws Exception {
        SamlUtils.initialize(logger);
        final Path certPath = getDataPath("saml.crt");
        final Certificate[] certs = CertParsingUtils.readCertificates(Collections.singletonList(certPath));
        if (certs.length != 1) {
            fail("Expected exactly 1 certificate in " + certPath);
        }
        if (certs[0] instanceof X509Certificate) {
            this.certificate = (X509Certificate) certs[0];
        } else {
            fail("Expected exactly X509Certificate, but was " + certs[0].getClass());
        }

        final Path threeCertsPath = getDataPath("saml-three-certs.crt");
        final Certificate[] threeCerts = CertParsingUtils.readCertificates(Collections.singletonList(threeCertsPath));
        if (threeCerts.length != 3) {
            fail("Expected exactly 3 certificate in " + certPath);
        }
        List<Class<?>> notX509Certificates = Arrays.stream(threeCerts).filter((cert) -> {
            return (cert instanceof X509Certificate) == false;
        }).map(cert -> cert.getClass()).collect(Collectors.toList());
        if (notX509Certificates.isEmpty() == false) {
            fail("Expected exactly X509Certificates, but found " + notX509Certificates);
        } else {
            this.threeCertificates = Arrays.asList(threeCerts).toArray(new X509Certificate[0]);
        }
    }

    public void testBuildMinimalistMetadata() throws Exception {
        final EntityDescriptor descriptor = new SamlSpMetadataBuilder(Locale.getDefault(), "https://my.sp.example.net/")
                .assertionConsumerServiceUrl("https://my.sp.example.net/saml/acs/post")
                .build();

        final Element element = new EntityDescriptorMarshaller().marshall(descriptor);
        final String xml = SamlUtils.toString(element);
        assertThat(xml, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"https://my.sp.example.net/\">" +
                "<md:SPSSODescriptor AuthnRequestsSigned=\"false\" WantAssertionsSigned=\"true\"" +
                " protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">" +
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
                .encryptionCertificates(Arrays.asList(certificate))
                .organization("Hydra", "Hydra", "https://hail.hydra/")
                .withContact("administrative", "Wolfgang", "von Strucker", "baron.strucker@supreme.hydra")
                .withContact("technical", "Paul", "Ebersol", "pne@tech.hydra")
                .build();

        final Element element = new EntityDescriptorMarshaller().marshall(descriptor);
        final String xml = SamlUtils.toString(element);

        final String expectedCertificate = joinCertificateLines(
            "MIIDWDCCAkCgAwIBAgIVANRTZaFrK+Pz19O8TZsb3HSJmAWpMA0GCSqGSIb3DQEBCwUAMB0xGzAZ",
            "BgNVBAMTEkVsYXN0aWNzZWFyY2gtU0FNTDAeFw0xNzExMjkwMjQ3MjZaFw0yMDExMjgwMjQ3MjZa",
            "MB0xGzAZBgNVBAMTEkVsYXN0aWNzZWFyY2gtU0FNTDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC",
            "AQoCggEBALHTuPGOieCbD2mZUdYrdH4ofo7qFze6rQUROCLKqf69uBuwvraNWOcwxHUTKVlLMV3d",
            "dKzYo+yfC44AMXrrV+79xVWsTCNHu9sxQzcDwiEx2OtOOX9MAk6tJQ3svNrMPNXWh8ftwmmY9XdF",
            "ZwMYUdo6FPjSQj5uQTDmGWRgF08f7VRlk6N92d/fzn9DlDm+TFuaOr17OTSR4B6RTrNwKC29AmXQ",
            "TwCijCObjLqyMEqP20dZCQeVf2qw8JKUHhW4r6mCLzqmeR+kRTqiHMSWxJddzxDGw6X7fOS7iuzB",
            "0+TnsKwgu8nYrEXds9MkGf1Yco7WsM43g+Es+LhNHP+es70CAwEAAaOBjjCBizAdBgNVHQ4EFgQU",
            "ILqVKGhIi8p5Xffsow/IKFLhRbIwWQYDVR0jBFIwUIAUILqVKGhIi8p5Xffsow/IKFLhRbKhIaQf",
            "MB0xGzAZBgNVBAMTEkVsYXN0aWNzZWFyY2gtU0FNTIIVANRTZaFrK+Pz19O8TZsb3HSJmAWpMA8G",
            "A1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAGhl4V9mp4SWSV2E3HAJ1PX+Vmp6k27K",
            "d0tkOk1B9fyA13QB30teyiL7RR0vSHRyWFY8rQH1mHD366GKRWLITRG/QPULamGdYXX4h0pFj5ld",
            "aubLxM/O9vEAxOgmo/lsdkeIq9tLBqY06r/5A/Mcgo63KGi00AFYBoyvqfOu6nRLPnQr+rKVfdNO",
            "pWeIiFY1i2XTNZ3CZjNPSTwiQMUzrCxKXB9lL0vF6QL2Gj2iBhzNfXi88wf7xaR6XKY1wNuv3HLP",
            "sL7n+PWby7LRX188dyS1dmKfQcrKL65OssBA5NC8CAYyBiygBmWN+5kVJM5fSb0SwPSoVWrNyz+8",
            "IUldQE8="
        );

        final String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"https://kibana.apps.hydra/\">"
            + "  <md:SPSSODescriptor AuthnRequestsSigned=\"true\" WantAssertionsSigned=\"true\""
            + "      protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">"
            + "    <md:KeyDescriptor>"
            + "      <ds:KeyInfo xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">"
            + "        <ds:X509Data>"
            + "          <ds:X509Certificate>%(expectedCertificate)</ds:X509Certificate>"
            + "        </ds:X509Data>"
            + "      </ds:KeyInfo>"
            + "    </md:KeyDescriptor>"
            + "    <md:SingleLogoutService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\""
            + "        Location=\"https://kibana.apps.hydra/saml/logout\"/>"
            + "    <md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:persistent</md:NameIDFormat>"
            + "    <md:AssertionConsumerService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\""
            + "        Location=\"https://kibana.apps.hydra/saml/acs\" index=\"1\" isDefault=\"true\"/>"
            + "    <md:AttributeConsumingService index=\"1\" isDefault=\"true\">"
            + "      <md:ServiceName xml:lang=\"en-US\">Hydra Kibana</md:ServiceName>"
            + "      <md:RequestedAttribute"
            + "          FriendlyName=\"uid\" Name=\"urn:oid:0.9.2342.19200300.100.1.1\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "      <md:RequestedAttribute"
            + "          FriendlyName=\"mail\""
            + "          Name=\"urn:oid:0.9.2342.19200300.100.1.3\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "      <md:RequestedAttribute"
            + "          FriendlyName=\"groups\""
            + "          Name=\"urn:oid:1.3.6.1.4.1.5923.1.5.1.1\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "      <md:RequestedAttribute"
            + "          Name=\"urn:oid:2.16.840.1.113730.3.1.241\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "      <md:RequestedAttribute"
            + "          Name=\"urn:oid:1.3.6.1.4.1.5923.1.1.1.6\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "    </md:AttributeConsumingService>"
            + "  </md:SPSSODescriptor>"
            + "  <md:Organization>"
            + "    <md:OrganizationName xml:lang=\"en-US\">Hydra</md:OrganizationName>"
            + "      <md:OrganizationDisplayName xml:lang=\"en-US\">Hydra</md:OrganizationDisplayName>"
            + "    <md:OrganizationURL xml:lang=\"en-US\">https://hail.hydra/</md:OrganizationURL>"
            + "  </md:Organization>"
            + "  <md:ContactPerson contactType=\"administrative\">"
            + "    <md:GivenName>Wolfgang</md:GivenName>"
            + "    <md:SurName>von Strucker</md:SurName>"
            + "    <md:EmailAddress>baron.strucker@supreme.hydra</md:EmailAddress>"
            + "  </md:ContactPerson>"
            + "  <md:ContactPerson contactType=\"technical\">"
            + "    <md:GivenName>Paul</md:GivenName>"
            + "    <md:SurName>Ebersol</md:SurName>"
            + "    <md:EmailAddress>pne@tech.hydra</md:EmailAddress>"
            + "  </md:ContactPerson>"
            + "</md:EntityDescriptor>";

        final Map<String, Object> replacements = Map.of("expectedCertificate", expectedCertificate);
        final String expectedXmlWithCertificate = NamedFormatter.format(expectedXml, replacements);

        assertThat(xml, equalTo(normaliseXml(expectedXmlWithCertificate)));

        assertValidXml(xml);
    }

    public void testBuildFullMetadataWithSigningAndTwoEncryptionCerts() throws Exception {
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
                .signingCertificate(threeCertificates[0])
                .encryptionCertificates(Arrays.asList(threeCertificates[1], threeCertificates[2]))
                .organization("Hydra", "Hydra", "https://hail.hydra/")
                .withContact("administrative", "Wolfgang", "von Strucker", "baron.strucker@supreme.hydra")
                .withContact("technical", "Paul", "Ebersol", "pne@tech.hydra")
                .build();

        final Element element = new EntityDescriptorMarshaller().marshall(descriptor);
        final String xml = SamlUtils.toString(element);

        final String expectedCertificateOne = joinCertificateLines(
            "MIIDWDCCAkCgAwIBAgIVANRTZaFrK+Pz19O8TZsb3HSJmAWpMA0GCSqGSIb3DQEBCwUAMB0xGzAZ",
            "BgNVBAMTEkVsYXN0aWNzZWFyY2gtU0FNTDAeFw0xNzExMjkwMjQ3MjZaFw0yMDExMjgwMjQ3MjZa",
            "MB0xGzAZBgNVBAMTEkVsYXN0aWNzZWFyY2gtU0FNTDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC",
            "AQoCggEBALHTuPGOieCbD2mZUdYrdH4ofo7qFze6rQUROCLKqf69uBuwvraNWOcwxHUTKVlLMV3d",
            "dKzYo+yfC44AMXrrV+79xVWsTCNHu9sxQzcDwiEx2OtOOX9MAk6tJQ3svNrMPNXWh8ftwmmY9XdF",
            "ZwMYUdo6FPjSQj5uQTDmGWRgF08f7VRlk6N92d/fzn9DlDm+TFuaOr17OTSR4B6RTrNwKC29AmXQ",
            "TwCijCObjLqyMEqP20dZCQeVf2qw8JKUHhW4r6mCLzqmeR+kRTqiHMSWxJddzxDGw6X7fOS7iuzB",
            "0+TnsKwgu8nYrEXds9MkGf1Yco7WsM43g+Es+LhNHP+es70CAwEAAaOBjjCBizAdBgNVHQ4EFgQU",
            "ILqVKGhIi8p5Xffsow/IKFLhRbIwWQYDVR0jBFIwUIAUILqVKGhIi8p5Xffsow/IKFLhRbKhIaQf",
            "MB0xGzAZBgNVBAMTEkVsYXN0aWNzZWFyY2gtU0FNTIIVANRTZaFrK+Pz19O8TZsb3HSJmAWpMA8G",
            "A1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAGhl4V9mp4SWSV2E3HAJ1PX+Vmp6k27K",
            "d0tkOk1B9fyA13QB30teyiL7RR0vSHRyWFY8rQH1mHD366GKRWLITRG/QPULamGdYXX4h0pFj5ld",
            "aubLxM/O9vEAxOgmo/lsdkeIq9tLBqY06r/5A/Mcgo63KGi00AFYBoyvqfOu6nRLPnQr+rKVfdNO",
            "pWeIiFY1i2XTNZ3CZjNPSTwiQMUzrCxKXB9lL0vF6QL2Gj2iBhzNfXi88wf7xaR6XKY1wNuv3HLP",
            "sL7n+PWby7LRX188dyS1dmKfQcrKL65OssBA5NC8CAYyBiygBmWN+5kVJM5fSb0SwPSoVWrNyz+8",
            "IUldQE8="
        );

        final String expectedCertificateTwo = joinCertificateLines(
            "MIID0zCCArugAwIBAgIJALi5bDfjMszLMA0GCSqGSIb3DQEBCwUAMEgxDDAKBgNVBAoTA29yZzEW",
            "MBQGA1UECxMNZWxhc3RpY3NlYXJjaDEgMB4GA1UEAxMXRWxhc3RpY3NlYXJjaCBUZXN0IE5vZGUw",
            "HhcNMTUwOTIzMTg1MjU3WhcNMTkwOTIyMTg1MjU3WjBIMQwwCgYDVQQKEwNvcmcxFjAUBgNVBAsT",
            "DWVsYXN0aWNzZWFyY2gxIDAeBgNVBAMTF0VsYXN0aWNzZWFyY2ggVGVzdCBOb2RlMIIBIjANBgkq",
            "hkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3rGZ1QbsW0+MuyrSLmMfDFKtLBkIFW8V0gRuurFg1PUK",
            "KNR1Mq2tMVwjjYETAU/UY0iKZOzjgvYPKhDTYBTte/WHR1ZK4CYVv7TQX/gtFQG/ge/c7u0sLch9",
            "p7fbd+/HZiLS/rBEZDIohvgUvzvnA8+OIYnw4kuxKo/5iboAIS41klMg/lATm8V71LMY68inht71",
            "/ZkQoAHKgcR9z4yNYvQ1WqKG8DG8KROXltll3sTrKbl5zJhn660es/1ZnR6nvwt6xnSTl/mNHMjk",
            "fv1bs4rJ/py3qPxicdoSIn/KyojUcgHVF38fuAy2CQTdjVG5fWj9iz+mQvLm3+qsIYQdFwIDAQAB",
            "o4G/MIG8MAkGA1UdEwQCMAAwHQYDVR0OBBYEFEMMWLWQi/g83PzlHYqAVnty5L7HMIGPBgNVHREE",
            "gYcwgYSCCWxvY2FsaG9zdIIVbG9jYWxob3N0LmxvY2FsZG9tYWluggpsb2NhbGhvc3Q0ghdsb2Nh",
            "bGhvc3Q0LmxvY2FsZG9tYWluNIIKbG9jYWxob3N0NoIXbG9jYWxob3N0Ni5sb2NhbGRvbWFpbjaH",
            "BH8AAAGHEAAAAAAAAAAAAAAAAAAAAAEwDQYJKoZIhvcNAQELBQADggEBAMjGGXT8Nt1tbl2GkiKt",
            "miuGE2Ej66YuZ37WSJViaRNDVHLlg87TCcHek2rdO+6sFqQbbzEfwQ05T7xGmVu7tm54HwKMRugo",
            "Q3wct0bQC5wEWYN+oMDvSyO6M28mZwWb4VtR2IRyWP+ve5DHwTM9mxWa6rBlGzsQqH6YkJpZojzq",
            "k/mQTug+Y8aEmVoqRIPMHq9ob+S9qd5lp09+MtYpwPfTPx/NN+xMEooXWW/ARfpGhWPkg/FuCu4z",
            "1tFmCqHgNcWirzMm3dQpF78muE9ng6OB2MXQwL4VgnVkxmlZNHbkR2v/t8MyZJxCy4g6cTMM3S/U",
            "Mt5/+aIB2JAuMKyuD+A="
        );

        final String expectedCertificateThree = joinCertificateLines(
            "MIID1zCCAr+gAwIBAgIJALnUl/KSS74pMA0GCSqGSIb3DQEBCwUAMEoxDDAKBgNVBAoTA29yZzEW",
            "MBQGA1UECxMNZWxhc3RpY3NlYXJjaDEiMCAGA1UEAxMZRWxhc3RpY3NlYXJjaCBUZXN0IENsaWVu",
            "dDAeFw0xNTA5MjMxODUyNTVaFw0xOTA5MjIxODUyNTVaMEoxDDAKBgNVBAoTA29yZzEWMBQGA1UE",
            "CxMNZWxhc3RpY3NlYXJjaDEiMCAGA1UEAxMZRWxhc3RpY3NlYXJjaCBUZXN0IENsaWVudDCCASIw",
            "DQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMKm+P6vDAff0c6BWKGdhnYoNl9HijLIgfU3d9CQ",
            "cqKtwT+yUW3DPSVjIfaLmDIGj6Hl8jTHWPB7ZP4fzhrPi6m4qlRGclJMECBuNASZFiPDtEDv3mso",
            "eqOKQet6n7PZvgpWM7hxYZO4P1aMKJtRsFAdvBAdZUnv0spR5G4UZTHzSKmMeanIKFkLaD0XVKiL",
            "Qu9/z9M6roDQeAEoCJ/8JsanG8ih2ymfPHIZuNyYIOrVekHN2zU6bnVn8/PCeZSjS6h5xYw+Jl5g",
            "zGI/n+F5CZ+THoH8pM4pGp6xRVzpiH12gvERGwgSIDXdn/+uZZj+4lE7n2ENRSOt5KcOGG99r60C",
            "AwEAAaOBvzCBvDAJBgNVHRMEAjAAMB0GA1UdDgQWBBSSFhBXNp7AaNrHdlgCV0mCEzt7ajCBjwYD",
            "VR0RBIGHMIGEgglsb2NhbGhvc3SCFWxvY2FsaG9zdC5sb2NhbGRvbWFpboIKbG9jYWxob3N0NIIX",
            "bG9jYWxob3N0NC5sb2NhbGRvbWFpbjSCCmxvY2FsaG9zdDaCF2xvY2FsaG9zdDYubG9jYWxkb21h",
            "aW42hwR/AAABhxAAAAAAAAAAAAAAAAAAAAABMA0GCSqGSIb3DQEBCwUAA4IBAQANvAkddfLxn4/B",
            "CY4LY/1ET3d7ZRldjFTyjjHRYJ3CYBXWVahMskLxIcFNca8YjKfXoX8mcK+NQK/dAbGHXqk76yMl",
            "krKjh1OQiZ1YAX5ryYerGrZ99N3E9wnbn72bW3iumoLlqmTWlHEpMI0Ql6J75BQLTgKHxCPupVA5",
            "sTbWkKwGjXXAi84rUlzhDJOR8jk3/7ct0iZO8Hk6AWMcNix5Wka3IDGUXuEVevYRlxgVyCxcnZWC",
            "7JWREpar5aIPQFkY6VCEglxwUyXbHZw5T/u6XaKKnS7gz8RiwRh68ddSQJeEHi5e4onUD7bOCJgf",
            "siUwdiCkDbfN9Yum8OIpmBRs"
        );

        final String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"https://kibana.apps.hydra/\">"
            + "  <md:SPSSODescriptor AuthnRequestsSigned=\"true\""
            + "      WantAssertionsSigned=\"true\""
            + "      protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">"
            + "    <md:KeyDescriptor use=\"signing\">"
            + "      <ds:KeyInfo xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">"
            + "        <ds:X509Data>"
            + "          <ds:X509Certificate>%(expectedCertificateOne)</ds:X509Certificate>"
            + "        </ds:X509Data>"
            + "      </ds:KeyInfo>"
            + "    </md:KeyDescriptor>"
            + "    <md:KeyDescriptor use=\"encryption\">"
            + "      <ds:KeyInfo xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">"
            + "        <ds:X509Data>"
            + "          <ds:X509Certificate>%(expectedCertificateTwo)</ds:X509Certificate>"
            + "        </ds:X509Data>"
            + "      </ds:KeyInfo>"
            + "    </md:KeyDescriptor>"
            + "    <md:KeyDescriptor use=\"encryption\">"
            + "      <ds:KeyInfo xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">"
            + "       <ds:X509Data>"
            + "         <ds:X509Certificate>%(expectedCertificateThree)</ds:X509Certificate>"
            + "       </ds:X509Data>"
            + "      </ds:KeyInfo>"
            + "    </md:KeyDescriptor>"
            + "    <md:SingleLogoutService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\""
            + "        Location=\"https://kibana.apps.hydra/saml/logout\"/>"
            + "    <md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:persistent</md:NameIDFormat>"
            + "    <md:AssertionConsumerService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\""
            + "        Location=\"https://kibana.apps.hydra/saml/acs\""
            + "        index=\"1\""
            + "        isDefault=\"true\"/>"
            + "    <md:AttributeConsumingService index=\"1\" isDefault=\"true\">"
            + "      <md:ServiceName xml:lang=\"en-US\">Hydra Kibana</md:ServiceName>"
            + "      <md:RequestedAttribute"
            + "          FriendlyName=\"uid\""
            + "          Name=\"urn:oid:0.9.2342.19200300.100.1.1\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "      <md:RequestedAttribute"
            + "          FriendlyName=\"mail\""
            + "          Name=\"urn:oid:0.9.2342.19200300.100.1.3\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "      <md:RequestedAttribute"
            + "          FriendlyName=\"groups\""
            + "          Name=\"urn:oid:1.3.6.1.4.1.5923.1.5.1.1\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "      <md:RequestedAttribute"
            + "          Name=\"urn:oid:2.16.840.1.113730.3.1.241\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "      <md:RequestedAttribute"
            + "          Name=\"urn:oid:1.3.6.1.4.1.5923.1.1.1.6\""
            + "          NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:uri\"/>"
            + "    </md:AttributeConsumingService>"
            + "  </md:SPSSODescriptor>"
            + "  <md:Organization>"
            + "    <md:OrganizationName xml:lang=\"en-US\">Hydra</md:OrganizationName>"
            + "      <md:OrganizationDisplayName xml:lang=\"en-US\">Hydra</md:OrganizationDisplayName>"
            + "    <md:OrganizationURL xml:lang=\"en-US\">https://hail.hydra/</md:OrganizationURL>"
            + "  </md:Organization>"
            + "  <md:ContactPerson contactType=\"administrative\">"
            + "    <md:GivenName>Wolfgang</md:GivenName>"
            + "    <md:SurName>von Strucker</md:SurName>"
            + "    <md:EmailAddress>baron.strucker@supreme.hydra</md:EmailAddress>"
            + "  </md:ContactPerson>"
            + "  <md:ContactPerson contactType=\"technical\">"
            + "    <md:GivenName>Paul</md:GivenName>"
            + "    <md:SurName>Ebersol</md:SurName>"
            + "    <md:EmailAddress>pne@tech.hydra</md:EmailAddress>"
            + "  </md:ContactPerson>"
            + "</md:EntityDescriptor>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("expectedCertificateOne", expectedCertificateOne);
        replacements.put("expectedCertificateTwo", expectedCertificateTwo);
        replacements.put("expectedCertificateThree", expectedCertificateThree);

        final String expectedXmlWithCertificate = NamedFormatter.format(expectedXml, replacements);

        assertThat(xml, equalTo(normaliseXml(expectedXmlWithCertificate)));

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

    private String joinCertificateLines(String... lines) {
        return Arrays.stream(lines).collect(Collectors.joining(System.lineSeparator()));
    }

    private String normaliseXml(String input) {
        // Remove spaces between elements, and compress other spaces. These patterns don't use \s because
        // that would match newlines.
        return input.replaceAll("> +<", "><").replaceAll(" +", " ");
    }
}
