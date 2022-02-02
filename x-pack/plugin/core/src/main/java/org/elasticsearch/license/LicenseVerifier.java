/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
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
    public static boolean verifyLicense(final License license, PublicKey publicKey) {
        byte[] signedContent = null;
        byte[] publicKeyFingerprint = null;
        try {
            byte[] signatureBytes = Base64.getDecoder().decode(license.signature());
            ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
            @SuppressWarnings("unused")
            int version = byteBuffer.getInt();
            int magicLen = byteBuffer.getInt();
            byte[] magic = new byte[magicLen];
            byteBuffer.get(magic);
            int hashLen = byteBuffer.getInt();
            publicKeyFingerprint = new byte[hashLen];
            byteBuffer.get(publicKeyFingerprint);
            int signedContentLen = byteBuffer.getInt();
            signedContent = new byte[signedContentLen];
            byteBuffer.get(signedContent);
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            license.toXContent(contentBuilder, new ToXContent.MapParams(Collections.singletonMap(License.LICENSE_SPEC_VIEW_MODE, "true")));
            Signature rsa = Signature.getInstance("SHA512withRSA");
            rsa.initVerify(publicKey);
            BytesRefIterator iterator = BytesReference.bytes(contentBuilder).iterator();
            BytesRef ref;
            while ((ref = iterator.next()) != null) {
                rsa.update(ref.bytes, ref.offset, ref.length);
            }
            return rsa.verify(signedContent);
        } catch (IOException | NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            throw new IllegalStateException(e);
        } finally {
            if (signedContent != null) {
                Arrays.fill(signedContent, (byte) 0);
            }
        }
    }

    private static final PublicKey PUBLIC_KEY;

    static {
        try (InputStream is = LicenseVerifier.class.getResourceAsStream("/public.key")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            PUBLIC_KEY = CryptUtils.readPublicKey(out.toByteArray());
        } catch (IOException e) {
            throw new AssertionError("key file is part of the source and must deserialize correctly", e);
        }
    }

    public static boolean verifyLicense(final License license) {
        return verifyLicense(license, PUBLIC_KEY);
    }
}
