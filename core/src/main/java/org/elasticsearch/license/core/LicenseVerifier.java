/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.shaded.CryptUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Responsible for verifying signed licenses
 * The signed licenses are expected to have signatures with the appropriate spec
 * (see {@link org.elasticsearch.license.core.LicenseVerifier})
 * along with the appropriate encrypted public key
 */
public class LicenseVerifier {

    /**
     * Verifies Licenses using {@link #verifyLicense(License, byte[])} using the public key from the
     * resources
     */
    public static boolean verifyLicenses(final Collection<License> licenses) {
        final byte[] encryptedPublicKeyData = getPublicKeyContentFromResource("/public.key");
        try {
            for (License license : licenses) {
                if (!verifyLicense(license, encryptedPublicKeyData)) {
                    return false;
                }
            }
            return true;
        } finally {
            Arrays.fill(encryptedPublicKeyData, (byte) 0);
        }
    }

    /**
     * Verifies Licenses using {@link #verifyLicense(License, byte[])} using the provided public key
     */
    public static boolean verifyLicenses(final Collection<License> licenses, final Path publicKeyPath) throws IOException {
        final byte[] encryptedPublicKeyData = Files.readAllBytes(publicKeyPath);
        try {
            for (License license : licenses) {
                if (!verifyLicense(license, encryptedPublicKeyData)) {
                    return false;
                }
            }
            return true;
        } finally {
            Arrays.fill(encryptedPublicKeyData, (byte) 0);
        }
    }

    public static boolean verifyLicense(final License license) {
        final byte[] encryptedPublicKeyData = getPublicKeyContentFromResource("/public.key");
        try {
            return verifyLicense(license, encryptedPublicKeyData);
        } finally {
            Arrays.fill(encryptedPublicKeyData, (byte) 0);
        }
    }

    /**
     * verifies the license content with the signature and ensures that an expected public key is used
     * @param license to verify
     * @return true if valid, false otherwise
     */
    public static boolean verifyLicense(final License license, final byte[] encryptedPublicKeyData) {
        LicenseSignature licenseSignature = null;
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            license.toXContent(contentBuilder, new ToXContent.MapParams(Collections.singletonMap(Licenses.LICENSE_SPEC_VIEW_MODE, "true")));
            licenseSignature = parseSignature(license.signature());
            if(!verifyContent(encryptedPublicKeyData, contentBuilder.bytes().toBytes(), licenseSignature.contentSignature)) {
                 return false;
             }
            final byte[] hash = Base64.encodeBytesToBytes(encryptedPublicKeyData);
            return Arrays.equals(hash, licenseSignature.hash);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            if (licenseSignature != null) {
                licenseSignature.clear();
            }
        }
    }

    private static boolean verifyContent(byte[] encryptedPublicKeyData, byte[] data, byte[] contentSignature) {
        try {
            Signature rsa = Signature.getInstance("SHA512withRSA");
            rsa.initVerify(CryptUtils.readEncryptedPublicKey(encryptedPublicKeyData));
            rsa.update(data);
            return rsa.verify(contentSignature);
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] getPublicKeyContentFromResource(String resource) {
        try (InputStream inputStream = LicenseVerifier.class.getResourceAsStream(resource)) {
            return Streams.copyToByteArray(inputStream);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Signature structure:
     * | VERSION | MAGIC | PUB_KEY_DIGEST | SIGNED_LICENSE_CONTENT |
     */
    private static LicenseSignature parseSignature(String signature) throws IOException {
        byte[] signatureBytes = Base64.decode(signature);
        ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
        int version = byteBuffer.getInt();
        int magicLen = byteBuffer.getInt();
        byte[] magic = new byte[magicLen];
        byteBuffer.get(magic);
        int hashLen = byteBuffer.getInt();
        byte[] hash = new byte[hashLen];
        byteBuffer.get(hash);
        int signedContentLen = byteBuffer.getInt();
        byte[] signedContent = new byte[signedContentLen];
        byteBuffer.get(signedContent);
        return new LicenseSignature(version, hash, signedContent);
    }

    private static class LicenseSignature {
        private final int version;
        private final byte[] hash;
        private final byte[] contentSignature;

        private LicenseSignature(int version, byte[] hash, byte[] contentSignature) {
            this.version = version;
            this.hash = hash;
            this.contentSignature = contentSignature;
        }

        private void clear() {
            Arrays.fill(hash, (byte)0);
            Arrays.fill(contentSignature, (byte)0);
        }
    }
}
