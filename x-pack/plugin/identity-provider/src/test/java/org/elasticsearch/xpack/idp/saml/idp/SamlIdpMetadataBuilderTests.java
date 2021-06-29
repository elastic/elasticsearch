/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.idp;

import org.elasticsearch.common.util.NamedFormatter;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.impl.EntityDescriptorMarshaller;
import org.w3c.dom.Element;

import java.net.URL;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;
import static org.opensaml.saml.saml2.core.NameIDType.PERSISTENT;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class SamlIdpMetadataBuilderTests extends IdpSamlTestCase {

    private SamlFactory samlFactory;

    @Before
    public void setup() throws Exception {
        SamlInit.initialize();
        samlFactory = new SamlFactory();
    }

    public void testSimpleMetadataGeneration() throws Exception {
        final String entityId = "https://idp.org";
        final EntityDescriptor entityDescriptor = new SamlIdPMetadataBuilder(entityId)
            .withSingleSignOnServiceUrl(SAML2_REDIRECT_BINDING_URI, new URL(entityId + "/sso/redirect"))
            .build();
        final Element element = new EntityDescriptorMarshaller().marshall(entityDescriptor);
        final String xml = samlFactory.toString(element, false);
        assertThat(xml, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"https://idp.org\">" +
            "<md:IDPSSODescriptor WantAuthnRequestsSigned=\"false\" protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">" +
            "<md:SingleSignOnService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\" " +
            "Location=\"https://idp.org/sso/redirect\"/>" +
            "</md:IDPSSODescriptor>" +
            "</md:EntityDescriptor>"));
    }

    public void testMetadataGenerationWithAllParameters() throws Exception {
        final String entityId = "https://idp.org";
        final EntityDescriptor entityDescriptor = new SamlIdPMetadataBuilder(entityId)
            .withLocale(Locale.forLanguageTag("en"))
            .withSingleSignOnServiceUrl(SAML2_REDIRECT_BINDING_URI, new URL(entityId + "/sso/redirect"))
            .withSingleSignOnServiceUrl(SAML2_POST_BINDING_URI, new URL(entityId + "/sso/post"))
            .withSingleLogoutServiceUrl(SAML2_REDIRECT_BINDING_URI, new URL(entityId + "/slo/redirect"))
            .withSingleLogoutServiceUrl(SAML2_POST_BINDING_URI, new URL(entityId + "/slo/post"))
            .withNameIdFormat(PERSISTENT)
            .withNameIdFormat(TRANSIENT)
            .wantAuthnRequestsSigned(true)
            .withSigningCertificate(readCredentials("RSA", 2048).getEntityCertificate())
            .withSigningCertificate(readCredentials("RSA", 4096).getEntityCertificate())
            .withContact("technical", "Tony", "Stark", "tony@starkindustries.com")
            .organization("Avengers", "The Avengers", "https://linktotheidp.org")
            .build();
        final Element element = new EntityDescriptorMarshaller().marshall(entityDescriptor);
        final String xml = samlFactory.toString(element, false);

        // RSA_2048
        final String signingCertificateOne = joinCertificateLines(
            "MIIDYzCCAkugAwIBAgIVAITQVqXYYUT0w04Z2gWAZ6pv7gwbMA0GCSqGSIb3DQEBCwUAMDQxMjAw",
            "BgNVBAMTKUVsYXN0aWMgQ2VydGlmaWNhdGUgVG9vbCBBdXRvZ2VuZXJhdGVkIENBMCAXDTIwMDEy",
            "ODA2MzczNloYDzIxNjgxMDE5MDYzNzM2WjBRMRMwEQYKCZImiZPyLGQBGRYDb3JnMR0wGwYKCZIm",
            "iZPyLGQBGRYNZWxhc3RpY3NlYXJjaDEMMAoGA1UECxMDaWRwMQ0wCwYDVQQDEwR0ZXN0MIIBIjAN",
            "BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAujk+mVzI+qmf4gSJZdVVDdhFTi06kikb7FxG5JPu",
            "+gmU9Ke0LVEpP7Jp3gmhwsa18JUuvaepL1jnKmbbepKkEsvqUj4FuI/gImvFwb7X+xUwzNTYZAEv",
            "nZ4n16k0sBPuDuDibF0MGniVeLG3bD2VF3crFQrphFr+GZSXbVk5zIcSf6D6nSDcKmCNpVAK3jX9",
            "iV0nkr8cPtHOgprv1Y7mZgk5jwli9to0QD7r7OG5Db34R06JTMGTji+RULPISH1bc8FHdurRASkG",
            "msei5GlJqPSuKdViuaKPmDrvKR8OK6gMzUd3pikJgD8veLxEuZ640FHPndPlvwJrSLwhitRgPQID",
            "AQABo00wSzAdBgNVHQ4EFgQUD87H31WVQNfCc85/H2qhpzs3XfowHwYDVR0jBBgwFoAUVenAN+T0",
            "6rqNDxjMcvgimnTw+FgwCQYDVR0TBAIwADANBgkqhkiG9w0BAQsFAAOCAQEAIvHYxT30cvoHWUE2",
            "saDVJ4qs/e0G3WusDyem3e4HkqwLEah06RDSgVCaOfW3ey5Q6CIQW3HHGUYqO0nU8JVCWdAk3+bU",
            "YJJOeLnwD+SbDxxKBhxLdx+BjWata85lfJTR9+dXs0RXAAN8dSiIaj9NSgnwiJqQQZf7i66S7XB5",
            "8TDTdZlV3d26STLy5h7Uy6vyCka8Xu8HFQ4hH2qf2L6EhBbzVTB6tuyPQOQwrlLE65nhUNkfBbjZ",
            "lre45UMc9GuxzHkbvd3HEQaroMHZxnu+/n/JDlgsrCYUEXnZnOXvgUPupPynoRdDN1F6r95TLyU9",
            "pYjDf/6zNPE854VF6y1TqQ=="
            );
        // RSA_4096
        final String signingCertificateTwo = joinCertificateLines(
            "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"
            );

        final String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"https://idp.org\">"
            + "  <md:IDPSSODescriptor"
            + "      WantAuthnRequestsSigned=\"true\""
            + "      protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">"
            + "    <md:KeyDescriptor use=\"signing\">"
            + "      <ds:KeyInfo xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">"
            + "        <ds:X509Data>"
            + "          <ds:X509Certificate>%(signingCertificateOne)</ds:X509Certificate>"
            + "        </ds:X509Data>"
            + "      </ds:KeyInfo>"
            + "    </md:KeyDescriptor>"
            + "    <md:KeyDescriptor use=\"signing\">"
            + "      <ds:KeyInfo xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">"
            + "        <ds:X509Data>"
            + "          <ds:X509Certificate>%(signingCertificateTwo)</ds:X509Certificate>"
            + "        </ds:X509Data>"
            + "      </ds:KeyInfo>"
            + "    </md:KeyDescriptor>"
            + "    <md:SingleLogoutService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\""
            + "        Location=\"https://idp.org/slo/post\"/>"
            + "    <md:SingleLogoutService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\""
            + "        Location=\"https://idp.org/slo/redirect\"/>"
            + "    <md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:persistent</md:NameIDFormat>"
            + "    <md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:transient</md:NameIDFormat>"
            + "    <md:SingleSignOnService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\""
            + "        Location=\"https://idp.org/sso/post\"/>"
            + "    <md:SingleSignOnService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\""
            + "        Location=\"https://idp.org/sso/redirect\"/>"
            + "  </md:IDPSSODescriptor>"
            + "  <md:Organization>"
            + "    <md:OrganizationName xml:lang=\"en\">Avengers</md:OrganizationName>"
            + "    <md:OrganizationDisplayName xml:lang=\"en\">The Avengers</md:OrganizationDisplayName>"
            + "    <md:OrganizationURL xml:lang=\"en\">https://linktotheidp.org</md:OrganizationURL>"
            + "  </md:Organization>"
            + "  <md:ContactPerson contactType=\"technical\">"
            + "    <md:GivenName>Tony</md:GivenName>"
            + "    <md:SurName>Stark</md:SurName>"
            + "    <md:EmailAddress>tony@starkindustries.com</md:EmailAddress>"
            + "  </md:ContactPerson>"
            + "</md:EntityDescriptor>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("signingCertificateOne", signingCertificateOne);
        replacements.put("signingCertificateTwo", signingCertificateTwo);

        final String expectedXmlWithCertificate = NamedFormatter.format(expectedXml, replacements);

        assertThat(xml, Matchers.equalTo(normaliseXml(expectedXmlWithCertificate)));

        assertValidXml(xml);
    }

}
