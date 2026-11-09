/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.openssl.PEMEncryptor;
import org.bouncycastle.openssl.PEMException;
import org.elasticsearch.core.CharArrays;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Password based encryption for private keys written in PEM format.
 * The output is the legacy OpenSSL format ({@code Proc-Type: 4,ENCRYPTED} and {@code DEK-Info} headers): the key is derived from the
 * password and the first 8 bytes of the IV with OpenSSL's {@code EVP_BytesToKey} (MD5, one iteration) and the key material is encrypted
 * in CBC mode with PKCS#5 padding. This is the same derivation that {@code org.elasticsearch.common.ssl.PemUtils} uses to read such keys.
 * <p>
 * This is an opaque handle for a Bouncy Castle {@code PEMEncryptor} so that callers outside this module do not depend on
 * Bouncy Castle types, which are shaded into this module's jar.
 * </p>
 */
public final class PemEncryptor {

    private final PEMEncryptor encryptor;

    /**
     * @param algorithm the OpenSSL name of the encryption algorithm: {@code AES-128-CBC}, {@code AES-192-CBC}, {@code AES-256-CBC} or
     *                  {@code DES-EDE3-CBC}
     * @param password  the password from which the encryption key is derived
     * @throws IllegalArgumentException if {@code password} is {@code null} or empty
     */
    public PemEncryptor(String algorithm, char[] password) {
        if (password == null || password.length == 0) {
            throw new IllegalArgumentException("password empty");
        }
        record Params(String transformation, String keyAlgorithm, int keyLength, int ivLength) {}
        final Params params = switch (algorithm) {
            case "AES-128-CBC" -> new Params("AES/CBC/PKCS5Padding", "AES", 16, 16);
            case "AES-192-CBC" -> new Params("AES/CBC/PKCS5Padding", "AES", 24, 16);
            case "AES-256-CBC" -> new Params("AES/CBC/PKCS5Padding", "AES", 32, 16);
            case "DES-EDE3-CBC" -> new Params("DESede/CBC/PKCS5Padding", "DESede", 24, 8);
            default -> throw new IllegalArgumentException("unsupported PEM encryption algorithm [" + algorithm + "]");
        };
        final byte[] iv = new byte[params.ivLength];
        new SecureRandom().nextBytes(iv);

        this.encryptor = new PEMEncryptor() {
            @Override
            public String getAlgorithm() {
                return algorithm;
            }

            @Override
            public byte[] getIV() {
                return iv;
            }

            @Override
            public byte[] encrypt(byte[] encoding) throws PEMException {
                // Like Bouncy Castle's own encryptor, the key is derived when the key material is written, so that an unavailable
                // algorithm (e.g. MD5 in a FIPS JVM) surfaces as a PEMException from the write rather than from the construction.
                try {
                    final SecretKeySpec key = new SecretKeySpec(generateOpenSslKey(password, iv, params.keyLength), params.keyAlgorithm);
                    final Cipher cipher = Cipher.getInstance(params.transformation);
                    cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
                    return cipher.doFinal(encoding);
                } catch (GeneralSecurityException e) {
                    throw new PEMException("exception using cipher - please check password and data.", e);
                }
            }
        };
    }

    PEMEncryptor encryptor() {
        return encryptor;
    }

    /**
     * OpenSSL's {@code EVP_BytesToKey} with MD5 and a single iteration, as used for the legacy PEM encryption; the salt is the first 8
     * bytes of the IV. This mirrors {@code PemUtils#generateOpenSslKey}, which decrypts such keys.
     */
    private static byte[] generateOpenSslKey(char[] password, byte[] iv, int keyLength) throws NoSuchAlgorithmException {
        final byte[] passwordBytes = CharArrays.toUtf8Bytes(password);
        final MessageDigest md5 = MessageDigest.getInstance("MD5");
        final byte[] key = new byte[keyLength];
        int copied = 0;
        int remaining;
        while (copied < keyLength) {
            remaining = keyLength - copied;
            md5.update(passwordBytes, 0, passwordBytes.length);
            md5.update(iv, 0, 8); // AES IV (salt) is longer but we only need 8 bytes
            byte[] tempDigest = md5.digest();
            int bytesToCopy = (remaining > 16) ? 16 : remaining; // MD5 digests are 16 bytes
            System.arraycopy(tempDigest, 0, key, copied, bytesToCopy);
            copied += bytesToCopy;
            if (remaining == 0) {
                break;
            }
            md5.update(tempDigest, 0, 16); // use previous round digest as IV
        }
        Arrays.fill(passwordBytes, (byte) 0);
        return key;
    }
}
