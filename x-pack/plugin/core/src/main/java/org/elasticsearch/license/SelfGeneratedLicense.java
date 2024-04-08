/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;

import static org.elasticsearch.license.CryptUtils.decrypt;
import static org.elasticsearch.license.CryptUtils.decryptV3Format;
import static org.elasticsearch.license.CryptUtils.encrypt;
import static org.elasticsearch.license.CryptUtils.encryptV3Format;

class SelfGeneratedLicense {

    public static License create(License.Builder specBuilder) {
        return create(specBuilder, LicenseUtils.getMaxCompatibleLicenseVersion());
    }

    public static License create(License.Builder specBuilder, int version) {
        License spec = specBuilder.issuer("elasticsearch").version(version).build();
        final String signature;
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            spec.toXContent(contentBuilder, new ToXContent.MapParams(Collections.singletonMap(License.LICENSE_SPEC_VIEW_MODE, "true")));
            byte[] encrypt;
            if (version < License.VERSION_CRYPTO_ALGORITHMS) {
                encrypt = encryptV3Format(BytesReference.toBytes(BytesReference.bytes(contentBuilder)));
            } else {
                encrypt = encrypt(BytesReference.toBytes(BytesReference.bytes(contentBuilder)));
            }
            byte[] bytes = new byte[4 + 4 + encrypt.length];
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            // Set -version in signature
            byteBuffer.putInt(-version).putInt(encrypt.length).put(encrypt);
            signature = Base64.getEncoder().encodeToString(bytes);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return License.builder().fromLicenseSpec(spec, signature).build();
    }

    public static boolean verify(final License license) {
        try {
            byte[] signatureBytes = Base64.getDecoder().decode(license.signature());
            ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
            int version = byteBuffer.getInt();
            int contentLen = byteBuffer.getInt();
            byte[] content = new byte[contentLen];
            byteBuffer.get(content);
            final License expectedLicense;
            // Version in signature is -version, so check for -(-version) < 4
            byte[] decryptedContent = (-version < License.VERSION_CRYPTO_ALGORITHMS) ? decryptV3Format(content) : decrypt(content);
            // EMPTY is safe here because we don't call namedObject
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, decryptedContent)
            ) {
                parser.nextToken();
                expectedLicense = License.builder()
                    .fromLicenseSpec(License.fromXContent(parser), license.signature())
                    .version(-version)
                    .build();
            }
            return license.equals(expectedLicense);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static License.LicenseType validateSelfGeneratedType(License.LicenseType type) {
        switch (type) {
            case BASIC:
            case TRIAL:
                return type;
        }
        throw new IllegalArgumentException(
            "invalid self generated license type ["
                + type
                + "], only "
                + License.LicenseType.BASIC
                + " and "
                + License.LicenseType.TRIAL
                + " are accepted"
        );
    }
}
