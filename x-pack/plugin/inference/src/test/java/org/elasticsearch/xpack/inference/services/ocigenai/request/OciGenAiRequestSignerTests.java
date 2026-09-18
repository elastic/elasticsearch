/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class OciGenAiRequestSignerTests extends ESTestCase {

    private static final Instant FIXED_INSTANT = Instant.parse("2026-09-18T19:22:25Z");
    private static final String EXPECTED_DATE = "Fri, 18 Sep 2026 19:22:25 GMT";
    private static final URI EMBED_TEXT_URI = URI.create(
        "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com/20231130/actions/embedText"
    );
    private static final Pattern AUTHORIZATION_PATTERN = Pattern.compile(
        "Signature version=\"1\",keyId=\"([^\"]+)\",algorithm=\"rsa-sha256\",headers=\"([^\"]+)\",signature=\"([^\"]+)\""
    );

    public void testSign_PostRequest_ProducesAllSignedHeadersAndAValidSignature() throws Exception {
        var body = "{\"inputs\":[\"hello world\"]}".getBytes(StandardCharsets.UTF_8);
        var signer = createSigner();

        var headers = signer.sign("POST", EMBED_TEXT_URI, "application/json", body);

        assertThat(headers, aMapWithSize(4));
        assertThat(headers.get("date"), is(EXPECTED_DATE));
        assertThat(headers.get("host"), is("inference.generativeai.us-chicago-1.oci.oraclecloud.com"));
        assertThat(headers.get("x-content-sha256"), is(Base64.getEncoder().encodeToString(MessageDigests.sha256().digest(body))));

        var matcher = AUTHORIZATION_PATTERN.matcher(headers.get("authorization"));
        assertTrue("unexpected authorization header: " + headers.get("authorization"), matcher.matches());
        assertThat(matcher.group(1), is(OciGenAiTestUtils.keyId()));
        assertThat(matcher.group(2), is("(request-target) host date x-content-sha256 content-type content-length"));

        var signingString = String.join(
            "\n",
            "(request-target): post /20231130/actions/embedText",
            "host: inference.generativeai.us-chicago-1.oci.oraclecloud.com",
            "date: " + EXPECTED_DATE,
            "x-content-sha256: " + headers.get("x-content-sha256"),
            "content-type: application/json",
            "content-length: " + body.length
        );
        assertTrue("signature does not verify", verify(OciGenAiTestUtils.keyPair().getPublic(), signingString, matcher.group(3)));
    }

    public void testSign_GetRequest_SignsOnlyRequestTargetHostAndDate() throws Exception {
        var uri = URI.create("https://inference.generativeai.us-chicago-1.oci.oraclecloud.com/20231130/models?compartmentId=abc");
        var signer = createSigner();

        var headers = signer.sign("GET", uri, null, null);

        assertThat(headers.keySet(), containsInAnyOrder("date", "host", "authorization"));
        var matcher = AUTHORIZATION_PATTERN.matcher(headers.get("authorization"));
        assertTrue(matcher.matches());
        assertThat(matcher.group(2), is("(request-target) host date"));

        var signingString = String.join(
            "\n",
            "(request-target): get /20231130/models?compartmentId=abc",
            "host: inference.generativeai.us-chicago-1.oci.oraclecloud.com",
            "date: " + EXPECTED_DATE
        );
        assertTrue(verify(OciGenAiTestUtils.keyPair().getPublic(), signingString, matcher.group(3)));
    }

    public void testSign_HostHeaderIncludesNonDefaultPort() throws Exception {
        var headers = createSigner().sign(
            "POST",
            URI.create("http://127.0.0.1:12345/20231130/actions/chat"),
            "application/json",
            new byte[0]
        );

        assertThat(headers.get("host"), is("127.0.0.1:12345"));
    }

    public void testSign_EmptyBodyIsHashed() throws Exception {
        var headers = createSigner().sign("POST", EMBED_TEXT_URI, "application/json", null);

        // base64 of the SHA-256 hash of the empty input
        assertThat(headers.get("x-content-sha256"), is("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="));
    }

    public void testSign_KeyIdIsUsedVerbatim() throws Exception {
        var signer = new OciGenAiRequestSigner("ST$sometoken", OciGenAiTestUtils.keyPair().getPrivate(), fixedClock());

        var headers = signer.sign("POST", EMBED_TEXT_URI, "application/json", new byte[0]);

        assertThat(headers.get("authorization"), startsWith("Signature version=\"1\",keyId=\"ST$sometoken\","));
    }

    public void testSigningString_JoinsHeadersWithLineFeeds() {
        var headers = new LinkedHashMap<String, String>();
        headers.put("(request-target)", "post /a");
        headers.put("host", "h");

        var signingString = OciGenAiRequestSigner.signingString(headers);

        assertThat(signingString, is("(request-target): post /a\nhost: h"));
    }

    private static OciGenAiRequestSigner createSigner() {
        return new OciGenAiRequestSigner(OciGenAiTestUtils.keyId(), OciGenAiTestUtils.keyPair().getPrivate(), fixedClock());
    }

    private static Clock fixedClock() {
        return Clock.fixed(FIXED_INSTANT, ZoneOffset.UTC);
    }

    static boolean verify(PublicKey publicKey, String signingString, String base64Signature) throws GeneralSecurityException {
        var signature = Signature.getInstance("SHA256withRSA");
        signature.initVerify(publicKey);
        signature.update(signingString.getBytes(StandardCharsets.UTF_8));
        return signature.verify(Base64.getDecoder().decode(base64Signature));
    }
}
