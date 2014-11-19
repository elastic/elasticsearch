/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.util.*;

/**
 * Responsible for verifying signed licenses
 */
public class LicenseVerifier {

    /**
     * Verifies Licenses using {@link #verifyLicense(License)}
     */
    public static boolean verifyLicenses(final Collection<License> licenses) {
        for (License license : licenses) {
            if (!verifyLicense(license)) {
                return false;
            }
        }
        return true;
    }

    /**
     * verifies the license content with the signature and ensures that an expected public key is used
     * @param license to verify
     * @return true if valid, false otherwise
     */
    public static boolean verifyLicense(final License license) {
        LicenseSignature licenseSignature = null;
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            license.toXContent(contentBuilder, new ToXContent.MapParams(Collections.singletonMap(Licenses.LICENSE_SPEC_VIEW_MODE, "true")));
            licenseSignature = parseSignature(license.signature());
            if(!verifyContent(contentBuilder.bytes().toBytes(), licenseSignature.contentSignature)) {
                 return false;
             }
            final byte[] hash = Base64.encodeBytesToBytes(getPublicKeyContent("/public.key"));
            return Arrays.equals(hash, licenseSignature.hash);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            if (licenseSignature != null) {
                licenseSignature.clear();
            }
        }
    }

    private static boolean verifyContent(byte[] data, byte[] contentSignature) {
        try {
            Signature rsa = Signature.getInstance("SHA512withRSA");
            rsa.initVerify(CryptUtils.readEncryptedPublicKey(getPublicKeyContent("/public.key")));
            rsa.update(data);
            return rsa.verify(contentSignature);
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] getPublicKeyContent(String resource) {
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
