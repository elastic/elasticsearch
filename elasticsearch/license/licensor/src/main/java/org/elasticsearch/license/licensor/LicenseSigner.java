/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.CryptUtils;
import org.elasticsearch.license.core.License;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Collections;

/**
 * Responsible for generating a license signature according to
 * the signature spec and sign it with the provided encrypted private key
 */
public class LicenseSigner {

    private final static int MAGIC_LENGTH = 13;

    private final Path publicKeyPath;

    private final Path privateKeyPath;

    public LicenseSigner(final Path privateKeyPath, final Path publicKeyPath) {
        this.publicKeyPath = publicKeyPath;
        this.privateKeyPath = privateKeyPath;
    }

    /**
     * Generates a signature for the <code>licenseSpec</code>.
     * Signature structure:
     * | VERSION | MAGIC | PUB_KEY_DIGEST | SIGNED_LICENSE_CONTENT |
     *
     * @return a signed License
     */
    public License sign(License licenseSpec) throws IOException {
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        licenseSpec.toXContent(contentBuilder, new ToXContent.MapParams(Collections.singletonMap(License.LICENSE_SPEC_VIEW_MODE, "true")));
        final byte[] signedContent;
        try {
            final Signature rsa = Signature.getInstance("SHA512withRSA");
            rsa.initSign(CryptUtils.readEncryptedPrivateKey(Files.readAllBytes(privateKeyPath)));
            rsa.update(contentBuilder.bytes().toBytes());
            signedContent = rsa.sign();
        } catch (InvalidKeyException | IOException | NoSuchAlgorithmException | SignatureException e) {
            throw new IllegalStateException(e);
        }
        final byte[] magic = new byte[MAGIC_LENGTH];
        SecureRandom random = new SecureRandom();
        random.nextBytes(magic);
        final byte[] hash = Base64.encodeBytesToBytes(Files.readAllBytes(publicKeyPath));
        assert hash != null;
        byte[] bytes = new byte[4 + 4 + MAGIC_LENGTH + 4 + hash.length + 4 + signedContent.length];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.putInt(licenseSpec.version())
                .putInt(magic.length)
                .put(magic)
                .putInt(hash.length)
                .put(hash)
                .putInt(signedContent.length)
                .put(signedContent);

        return License.builder()
                .fromLicenseSpec(licenseSpec, Base64.encodeBytes(bytes))
                .build();
    }
}
