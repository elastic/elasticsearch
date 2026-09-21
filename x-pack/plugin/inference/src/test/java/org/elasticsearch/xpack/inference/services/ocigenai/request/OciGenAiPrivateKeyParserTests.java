/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;

import java.security.InvalidKeyException;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPrivateKey;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OciGenAiPrivateKeyParserTests extends ESTestCase {

    public void testParse_Pkcs8Pem() throws Exception {
        var expected = OciGenAiTestUtils.keyPair().getPrivate();

        var parsed = OciGenAiPrivateKeyParser.parse(OciGenAiTestUtils.toPkcs8Pem(expected));

        assertThat(parsed, instanceOf(RSAPrivateKey.class));
        assertArrayEquals(expected.getEncoded(), parsed.getEncoded());
    }

    public void testParse_Pkcs1Pem() throws Exception {
        var expected = (RSAPrivateCrtKey) OciGenAiTestUtils.keyPair().getPrivate();

        var parsed = OciGenAiPrivateKeyParser.parse(OciGenAiTestUtils.toPkcs1Pem(expected));

        assertThat(parsed, instanceOf(RSAPrivateKey.class));
        var rsaKey = (RSAPrivateKey) parsed;
        assertThat(rsaKey.getModulus(), is(expected.getModulus()));
        assertThat(rsaKey.getPrivateExponent(), is(expected.getPrivateExponent()));
    }

    public void testParse_ToleratesWindowsLineEndingsAndSurroundingWhitespace() throws Exception {
        var expected = OciGenAiTestUtils.keyPair().getPrivate();
        var pem = "  \n" + OciGenAiTestUtils.toPkcs8Pem(expected).replace("\n", "\r\n") + "\n\n";

        var parsed = OciGenAiPrivateKeyParser.parse(pem);

        assertArrayEquals(expected.getEncoded(), parsed.getEncoded());
    }

    public void testParse_RejectsEncryptedPkcs8Key() {
        var exception = expectThrows(
            InvalidKeyException.class,
            () -> OciGenAiPrivateKeyParser.parse("-----BEGIN ENCRYPTED PRIVATE KEY-----\nabcd\n-----END ENCRYPTED PRIVATE KEY-----")
        );

        assertThat(exception.getMessage(), is(OciGenAiPrivateKeyParser.ENCRYPTED_KEY_MESSAGE));
    }

    public void testParse_RejectsEncryptedPkcs1Key() {
        var exception = expectThrows(
            InvalidKeyException.class,
            () -> OciGenAiPrivateKeyParser.parse(
                "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,ABC\n\nabcd\n-----END RSA PRIVATE KEY-----"
            )
        );

        assertThat(exception.getMessage(), is(OciGenAiPrivateKeyParser.ENCRYPTED_KEY_MESSAGE));
    }

    public void testParse_RejectsUnknownFormat() {
        var exception = expectThrows(InvalidKeyException.class, () -> OciGenAiPrivateKeyParser.parse("not a pem"));

        assertThat(exception.getMessage(), is(OciGenAiPrivateKeyParser.UNSUPPORTED_FORMAT_MESSAGE));
    }

    public void testParse_RejectsMissingFooter() {
        var exception = expectThrows(
            InvalidKeyException.class,
            () -> OciGenAiPrivateKeyParser.parse("-----BEGIN PRIVATE KEY-----\nabcd\n")
        );

        assertThat(exception.getMessage(), is(OciGenAiPrivateKeyParser.UNSUPPORTED_FORMAT_MESSAGE));
    }

    public void testParse_RejectsInvalidBase64() {
        var exception = expectThrows(
            InvalidKeyException.class,
            () -> OciGenAiPrivateKeyParser.parse("-----BEGIN PRIVATE KEY-----\n@@@@\n-----END PRIVATE KEY-----")
        );

        assertThat(exception.getMessage(), containsString("not valid base64"));
    }

    public void testParse_RejectsNull() {
        expectThrows(InvalidKeyException.class, () -> OciGenAiPrivateKeyParser.parse(null));
    }
}
