/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.bc.BcPEMDecryptorProvider;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * {@link PemEncryptor} implements the legacy OpenSSL PEM encryption itself (on JCA/JCE primitives), so its output is checked against two
 * independent implementations of that format: the Bouncy Castle lightweight decryptor and Elasticsearch's own {@link PemUtils}.
 */
public class PemEncryptorTests extends ESTestCase {

    private static final List<String> SUPPORTED_ALGORITHMS = List.of("AES-128-CBC", "AES-192-CBC", "AES-256-CBC", "DES-EDE3-CBC");

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
    }

    public void testEncryptedKeyIsReadableByIndependentImplementations() throws Exception {
        final KeyPair keyPair = CertGenUtils.generateKeyPair(2048);
        final char[] password = randomAlphaOfLengthBetween(8, 32).toCharArray();
        for (String algorithm : SUPPORTED_ALGORITHMS) {
            final String pem = writeEncryptedKey(keyPair.getPrivate(), new PemEncryptor(algorithm, password));
            assertThat(algorithm, pem, startsWith("-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: " + algorithm + ","));

            assertThat(algorithm, decryptWithBouncyCastle(pem, password), equalTo(keyPair.getPrivate().getEncoded()));
            assertThat(algorithm, decryptWithPemUtils(pem, password).getEncoded(), equalTo(keyPair.getPrivate().getEncoded()));
        }
    }

    public void testWrongPasswordIsRejected() throws Exception {
        final KeyPair keyPair = CertGenUtils.generateKeyPair(2048);
        final char[] password = randomAlphaOfLengthBetween(8, 32).toCharArray();
        final char[] wrongPassword = (new String(password) + randomAlphaOfLength(1)).toCharArray();
        for (String algorithm : SUPPORTED_ALGORITHMS) {
            final String pem = writeEncryptedKey(keyPair.getPrivate(), new PemEncryptor(algorithm, password));
            // a wrong key fails the padding check, or in rare cases yields bytes that are not a valid key; either way it does not decrypt
            expectThrows(Exception.class, () -> decryptWithBouncyCastle(pem, wrongPassword));
            expectThrows(Exception.class, () -> decryptWithPemUtils(pem, wrongPassword));
        }
    }

    public void testUnsupportedAlgorithm() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new PemEncryptor(randomFrom("DES-CBC", "AES-128-CFB", "RC2-CBC", randomAlphaOfLength(8)), "password".toCharArray())
        );
        assertThat(e.getMessage(), containsString("unsupported PEM encryption algorithm"));
    }

    private static String writeEncryptedKey(PrivateKey privateKey, PemEncryptor encryptor) throws IOException {
        final StringWriter out = new StringWriter();
        try (PemWriter writer = new PemWriter(out)) {
            writer.writeObject(privateKey, encryptor);
        }
        return out.toString();
    }

    /**
     * Decrypts with Bouncy Castle's lightweight (non JCA) implementation of the legacy PEM encryption.
     *
     * @return the PKCS#8 encoding of the private key
     */
    private static byte[] decryptWithBouncyCastle(String pem, char[] password) throws IOException {
        try (PEMParser parser = new PEMParser(new StringReader(pem))) {
            final Object object = parser.readObject();
            assertThat(object, instanceOf(PEMEncryptedKeyPair.class));
            final PEMKeyPair keyPair = ((PEMEncryptedKeyPair) object).decryptKeyPair(new BcPEMDecryptorProvider(password));
            return keyPair.getPrivateKeyInfo().getEncoded();
        }
    }

    private PrivateKey decryptWithPemUtils(String pem, char[] password) throws Exception {
        final Path path = createTempFile("key", ".pem");
        Files.writeString(path, pem, StandardCharsets.UTF_8);
        return PemUtils.readPrivateKey(path, () -> password);
    }
}
