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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;

public class SamlLogoutResponseHandlerHttpPostTests extends SamlResponseHandlerTests {

    private SamlLogoutResponseHandler samlLogoutResponseHandler;

    @Before
    public void setupHandler() {
        clock = new ClockMock();
        maxSkew = TimeValue.timeValueMinutes(1);
        requestId = randomId();
        samlLogoutResponseHandler = new SamlLogoutResponseHandler(clock,
            getIdpConfiguration(() -> buildOpenSamlCredential(idpSigningCertificatePair)),
            getSpConfiguration(emptyList()),
            maxSkew);
    }

    public void testHandlerWorksWithHttpPostBinding() throws Exception {
        final String payload = buildLogoutResponsePayload(emptyMap(), true);
        samlLogoutResponseHandler.handle(payload, List.of(requestId));
    }

    public void testHandlerFailsWithHttpPostBindingAndNoSignature() throws Exception {
        final String payload = buildLogoutResponsePayload(emptyMap(), false);
        final ElasticsearchSecurityException e = expectSamlException(() -> samlLogoutResponseHandler.handle(payload, List.of(requestId)));
        assertThat(e.getMessage(), containsString("is not signed"));
    }

    public void testHandlerWillThrowWhenStatusIsNotSuccess() throws Exception {
        final Map<String, Object> replacements = new HashMap<>();
        replacements.put("status", "urn:oasis:names:tc:SAML:2.0:status:Requester");
        final String payload = buildLogoutResponsePayload(replacements, true);
        final ElasticsearchSecurityException e =
            expectSamlException(() -> samlLogoutResponseHandler.handle(payload, List.of(requestId)));
        assertThat(e.getMessage(), containsString("not a 'success' response"));
    }

    private String buildLogoutResponsePayload(Map<String, Object> data, boolean shouldSign) throws Exception {
        final String template = "<?xml version=\"1.0\"?>\n"
            + "<samlp:LogoutResponse xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" \n"
            + "                      ID=\"%(randomId)\"\n"
            + "                      InResponseTo=\"%(requestId)\" Version=\"2.0\" \n"
            + "                      IssueInstant=\"%(now)\"\n"
            + "                      Destination=\"%(SP_LOGOUT_URL)\">\n"
            + "    <saml:Issuer xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\">%(IDP_ENTITY_ID)</saml:Issuer>\n"
            + "    <samlp:Status>\n"
            + "        <samlp:StatusCode Value=\"%(status)\"/>\n"
            + "    </samlp:Status>\n"
            + "</samlp:LogoutResponse>";

        Map<String, Object> replacements = new HashMap<>(data);
        replacements.putIfAbsent("IDP_ENTITY_ID", IDP_ENTITY_ID);
        replacements.putIfAbsent("now", clock.instant());
        replacements.putIfAbsent("randomId", requestId);
        replacements.putIfAbsent("requestId", requestId);
        replacements.putIfAbsent("SP_LOGOUT_URL", SP_LOGOUT_URL);
        replacements.putIfAbsent("status", "urn:oasis:names:tc:SAML:2.0:status:Success");
        final String xml = shouldSign
            ? signDoc(NamedFormatter.format(template, replacements)) : NamedFormatter.format(template, replacements);
        String encoded = URLEncoder.encode(Base64.getEncoder().encodeToString(xml.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.US_ASCII.name());
        return String.format(Locale.ROOT, "SAMLResponse=%s", encoded);
    }
}
