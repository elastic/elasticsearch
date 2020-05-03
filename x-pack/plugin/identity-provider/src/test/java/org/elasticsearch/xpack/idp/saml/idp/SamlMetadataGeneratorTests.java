/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.saml.idp;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.NamedFormatter;
import org.elasticsearch.xpack.idp.action.SamlMetadataResponse;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.hamcrest.Matchers;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.security.impl.SAMLSignatureProfileValidator;

import org.opensaml.xmlsec.signature.support.SignatureException;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureValidator;
import org.w3c.dom.Element;

import java.net.URL;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;

public class SamlMetadataGeneratorTests extends IdpSamlTestCase {

    public void testGenerateMetadata() throws Exception {
        SamlServiceProvider sp = mock(SamlServiceProvider.class);
        when(sp.getEntityId()).thenReturn("https://sp.org");
        when(sp.shouldSignAuthnRequests()).thenReturn(true);
        SamlIdentityProvider idp = mock(SamlIdentityProvider.class);
        when(idp.getEntityId()).thenReturn("https://idp.org");
        when(idp.getMetadataSigningCredential()).thenReturn(null);
        when(idp.getSigningCredential()).thenReturn(readCredentials("RSA", 2048));
        when(idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI)).thenReturn(new URL("https://idp.org/sso/redirect"));
        when(idp.getSingleLogoutEndpoint(SAML2_POST_BINDING_URI)).thenReturn(new URL("https://idp.org/slo/post"));
        mockRegisteredServiceProvider(idp, "https://sp.org", sp);

        SamlFactory factory = new SamlFactory();
        SamlMetadataGenerator generator = new SamlMetadataGenerator(factory, idp);
        PlainActionFuture<SamlMetadataResponse> future = new PlainActionFuture<>();
        generator.generateMetadata("https://sp.org", null, future);
        SamlMetadataResponse response = future.actionGet();
        final String xml = response.getXmlString();

        // RSA_2048
        final String signingCertificate = joinCertificateLines(
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

        final String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"https://idp.org\">"
            + "  <md:IDPSSODescriptor"
            + "      WantAuthnRequestsSigned=\"true\""
            + "      protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">"
            + "    <md:KeyDescriptor use=\"signing\">"
            + "      <ds:KeyInfo xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">"
            + "        <ds:X509Data>"
            + "          <ds:X509Certificate>%(signingCertificate)</ds:X509Certificate>"
            + "        </ds:X509Data>"
            + "      </ds:KeyInfo>"
            + "    </md:KeyDescriptor>"
            + "    <md:SingleLogoutService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\""
            + "        Location=\"https://idp.org/slo/post\"/>"
            + "    <md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:persistent</md:NameIDFormat>"
            + "    <md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:transient</md:NameIDFormat>"
            + "    <md:SingleSignOnService"
            + "        Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\""
            + "        Location=\"https://idp.org/sso/redirect\"/>"
            + "  </md:IDPSSODescriptor>"
            + "</md:EntityDescriptor>";

        final Map<String, Object> replacements = Map.of("signingCertificate", signingCertificate);

        final String expectedXmlWithCertificate = NamedFormatter.format(expectedXml, replacements);
        assertThat(xml, Matchers.equalTo(normaliseXml(expectedXmlWithCertificate)));
        assertValidXml(xml);
    }

    public void testGenerateAndSignMetadata() throws Exception {
        SamlServiceProvider sp = mock(SamlServiceProvider.class);
        when(sp.getEntityId()).thenReturn("https://sp.org");
        when(sp.shouldSignAuthnRequests()).thenReturn(true);
        SamlIdentityProvider idp = mock(SamlIdentityProvider.class);
        when(idp.getEntityId()).thenReturn("https://idp.org");
        when(idp.getMetadataSigningCredential()).thenReturn(readCredentials("RSA", 4096));
        when(idp.getSigningCredential()).thenReturn(readCredentials("RSA", 2048));
        when(idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI)).thenReturn(new URL("https://idp.org/sso/redirect"));
        when(idp.getSingleLogoutEndpoint(SAML2_POST_BINDING_URI)).thenReturn(new URL("https://idp.org/slo/post"));
        mockRegisteredServiceProvider(idp, "https://sp.org", sp);

        X509Credential signingCredential = readCredentials("RSA", 4096);
        SamlFactory factory = new SamlFactory();
        SamlMetadataGenerator generator = new SamlMetadataGenerator(factory, idp);
        Element element = generator.possiblySignDescriptor(generator.buildEntityDescriptor(sp), signingCredential);
        EntityDescriptor descriptor = factory.buildXmlObject(element, EntityDescriptor.class);
        Signature signature = descriptor.getSignature();
        assertNotNull(signature);
        SAMLSignatureProfileValidator profileValidator = new SAMLSignatureProfileValidator();
        profileValidator.validate(signature);
        SignatureValidator.validate(signature, signingCredential);
        //no exception thrown
        SignatureException e = expectThrows(SignatureException.class,
            () -> SignatureValidator.validate(signature, readCredentials("RSA", 2048)));
        if (inFipsJvm()) {
            assertThat(e.getMessage(), containsString("Signature cryptographic validation not successful"));
        } else {
            assertThat(e.getMessage(), containsString("Unable to evaluate key against signature"));
        }
    }

}
