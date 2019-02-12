/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;

import static org.elasticsearch.license.CryptUtils.encryptV3Format;
import static org.elasticsearch.license.CryptUtils.encrypt;
import static org.elasticsearch.license.CryptUtils.decryptV3Format;
import static org.elasticsearch.license.CryptUtils.decrypt;

class SelfGeneratedLicense {

    public static License create(License.Builder specBuilder, DiscoveryNodes currentNodes) {
        return create(specBuilder, LicenseUtils.compatibleLicenseVersion(currentNodes));
    }

    public static License create(License.Builder specBuilder, int version) {
        License spec = specBuilder
                .issuer("elasticsearch")
                .version(version)
                .build();
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
            byteBuffer.putInt(-version)
                    .putInt(encrypt.length)
                    .put(encrypt);
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
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, decryptedContent)) {
                parser.nextToken();
                expectedLicense = License.builder().fromLicenseSpec(License.fromXContent(parser),
                        license.signature()).version(-version).build();
            }
            return license.equals(expectedLicense);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean validSelfGeneratedType(String type) {
        return "basic".equals(type) || "trial".equals(type);
    }
}
