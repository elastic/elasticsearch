/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.NamedFormatter;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.containsString;

public class SamlLogoutResponseHandlerTests extends SamlResponseHandlerTests {

    private SamlLogoutResponseHandler samlLogoutResponseHandler;

    @Before
    public void setupHandler() {
        clock = new ClockMock();
        maxSkew = TimeValue.timeValueMinutes(1);
        requestId = randomId();
        samlLogoutResponseHandler = new SamlLogoutResponseHandler(
            clock,
            getIdpConfiguration(() -> buildOpenSamlCredential(idpSigningCertificatePair)),
            getSpConfiguration(emptyList()),
            maxSkew);
    }

    public void testHandleWorksWithoutSignature() {
        final String xml = "<?xml version=\"1.0\"?>\n"
            + "<samlp:LogoutResponse xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" \n"
            + "                      ID=\"%(randomId)\"\n"
            + "                      InResponseTo=\"%(requestId)\" Version=\"2.0\" \n"
            + "                      IssueInstant=\"%(now)\"\n"
            + "                      Destination=\"%(SP_LOGOUT_URL)\">\n"
            + "    <saml:Issuer xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\">%(IDP_ENTITY_ID)</saml:Issuer>\n"
            + "    <samlp:Status>\n"
            + "        <samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/>\n"
            + "    </samlp:Status>\n"
            + "</samlp:LogoutResponse>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("now", clock.instant());
        replacements.put("randomId", requestId);
        replacements.put("requestId", requestId);
        replacements.put("SP_LOGOUT_URL", SP_LOGOUT_URL);

        final String payload = NamedFormatter.format(xml, replacements);
        samlLogoutResponseHandler.handle(payload.getBytes(StandardCharsets.UTF_8), List.of(requestId));
    }

    public void testHandleWorksWithSignature() throws Exception {
        final String xml = "<?xml version=\"1.0\"?>\n"
            + "<samlp:LogoutResponse xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" \n"
            + "                      ID=\"%(randomId)\"\n"
            + "                      InResponseTo=\"%(requestId)\" Version=\"2.0\" \n"
            + "                      IssueInstant=\"%(now)\"\n"
            + "                      Destination=\"%(SP_LOGOUT_URL)\">\n"
            + "    <saml:Issuer xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\">%(IDP_ENTITY_ID)</saml:Issuer>\n"
            + "    <samlp:Status>\n"
            + "        <samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/>\n"
            + "    </samlp:Status>\n"
            + "</samlp:LogoutResponse>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("now", clock.instant());
        replacements.put("randomId", requestId);
        replacements.put("requestId", requestId);
        replacements.put("SP_LOGOUT_URL", SP_LOGOUT_URL);

        final String payload = signDoc(NamedFormatter.format(xml, replacements));
        samlLogoutResponseHandler.handle(payload.getBytes(StandardCharsets.UTF_8), List.of(requestId));
    }

    public void testHandleWillThrowWhenStatusIsNotSuccess() {
        final String xml = "<?xml version=\"1.0\"?>\n"
            + "<samlp:LogoutResponse xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" \n"
            + "                      ID=\"%(randomId)\"\n"
            + "                      InResponseTo=\"%(requestId)\" Version=\"2.0\" \n"
            + "                      IssueInstant=\"%(now)\"\n"
            + "                      Destination=\"%(SP_LOGOUT_URL)\">\n"
            + "    <saml:Issuer xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\">%(IDP_ENTITY_ID)</saml:Issuer>\n"
            + "    <samlp:Status>\n"
            + "        <samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Requester\"/>\n"
            + "    </samlp:Status>\n"
            + "</samlp:LogoutResponse>";

        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.put("now", clock.instant());
        replacements.put("randomId", requestId);
        replacements.put("requestId", requestId);
        replacements.put("SP_LOGOUT_URL", SP_LOGOUT_URL);

        final String payload = NamedFormatter.format(xml, replacements);
        final ElasticsearchSecurityException e = expectSamlException(() ->
            samlLogoutResponseHandler.handle(payload.getBytes(StandardCharsets.UTF_8), List.of(requestId)));
        assertThat(e.getMessage(), containsString("not a 'success' response"));
    }
}
