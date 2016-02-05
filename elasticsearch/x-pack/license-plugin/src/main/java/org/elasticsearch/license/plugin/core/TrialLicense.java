/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.License;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.elasticsearch.license.core.CryptUtils.decrypt;
import static org.elasticsearch.license.core.CryptUtils.encrypt;

public class TrialLicense {

    public static License create(License.Builder specBuilder) {
        License spec = specBuilder
                .type("trial")
                .issuer("elasticsearch")
                .version(License.VERSION_CURRENT)
                .build();
        final String signature;
        try {
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
            spec.toXContent(contentBuilder, new ToXContent.MapParams(Collections.singletonMap(License.LICENSE_SPEC_VIEW_MODE, "true")));
            byte[] encrypt = encrypt(contentBuilder.bytes().toBytes());
            byte[] bytes = new byte[4 + 4 + encrypt.length];
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            // always generate license version -VERSION_CURRENT
            byteBuffer.putInt(-License.VERSION_CURRENT)
                    .putInt(encrypt.length)
                    .put(encrypt);
            signature = Base64.encodeBytes(bytes);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return License.builder().fromLicenseSpec(spec, signature).build();
    }

    public static boolean verify(final License license) {
        try {
            byte[] signatureBytes = Base64.decode(license.signature());
            ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
            int version = byteBuffer.getInt();
            int contentLen = byteBuffer.getInt();
            byte[] content = new byte[contentLen];
            byteBuffer.get(content);
            final License expectedLicense;
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(decrypt(content))) {
                parser.nextToken();
                expectedLicense = License.builder().fromLicenseSpec(License.fromXContent(parser),
                        license.signature()).version(-version).build();
            }
            return license.equals(expectedLicense);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
