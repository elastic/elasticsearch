/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

/**
 * Responsible for verifying signed licenses
 */
public class LicenseVerifier {

    /**
     * verifies the license content with the signature using the packaged
     * public key
     * @param license to verify
     * @return true if valid, false otherwise
     */
    public static boolean verifyLicense(final License license, byte[] encryptedPublicKeyData) {
        byte[] signedContent = null;
        byte[] signatureHash = null;
        try {
            byte[] signatureBytes = Base64.getDecoder().decode(license.signature());
            ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
            int version = byteBuffer.getInt();
            int magicLen = byteBuffer.getInt();
            byte[] magic = new byte[magicLen];
            byteBuffer.get(magic);
            int hashLen = byteBuffer.getInt();
            signatureHash = new byte[hashLen];
            byteBuffer.get(signatureHash);
            int signedContentLen = byteBuffer.getInt();
            signedContent = new byte[signedContentLen];
            byteBuffer.get(signedContent);
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            license.toXContent(contentBuilder, new ToXContent.MapParams(Collections.singletonMap(License.LICENSE_SPEC_VIEW_MODE, "true")));
            Signature rsa = Signature.getInstance("SHA512withRSA");
            rsa.initVerify(CryptUtils.readEncryptedPublicKey(encryptedPublicKeyData));
            BytesRefIterator iterator = BytesReference.bytes(contentBuilder).iterator();
            BytesRef ref;
            while((ref = iterator.next()) != null) {
                rsa.update(ref.bytes, ref.offset, ref.length);
            }
            return rsa.verify(signedContent)
                    && Arrays.equals(Base64.getEncoder().encode(encryptedPublicKeyData), signatureHash);
        } catch (IOException | NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            throw new IllegalStateException(e);
        } finally {
            Arrays.fill(encryptedPublicKeyData, (byte) 0);
            if (signedContent != null) {
                Arrays.fill(signedContent, (byte) 0);
            }
            if (signatureHash != null) {
                Arrays.fill(signatureHash, (byte) 0);
            }
        }
    }

    public static boolean verifyLicense(final License license) {
        final byte[] publicKeyBytes;
        try (InputStream is = LicenseVerifier.class.getResourceAsStream("/public.key")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            publicKeyBytes = out.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        return verifyLicense(license, publicKeyBytes);
    }
}
