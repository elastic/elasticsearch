/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.ESLicense;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class TrialLicenseUtils {

    public static TrialLicenseBuilder builder() {
        return new TrialLicenseBuilder();
    }

    public static class TrialLicenseBuilder {
        private static final String DEFAULT_ISSUER = "elasticsearch";
        private static final String DEFAULT_TYPE = "trial";
        private static final String DEFAULT_SUBSCRIPTION_TYPE = "none";

        private String feature;
        private long expiryDate = -1;
        private long issueDate = -1;
        private TimeValue duration;
        private int maxNodes = -1;
        private String uid = null;
        private String issuedTo;

        public TrialLicenseBuilder() {
        }

        public TrialLicenseBuilder uid(String uid) {
            this.uid = uid;
            return this;
        }

        public TrialLicenseBuilder issuedTo(String issuedTo) {
            this.issuedTo = issuedTo;
            return this;
        }

        public TrialLicenseBuilder maxNodes(int maxNodes) {
            this.maxNodes = maxNodes;
            return this;
        }

        public TrialLicenseBuilder feature(String featureType) {
            this.feature = featureType;
            return this;
        }

        public TrialLicenseBuilder issueDate(long issueDate) {
            this.issueDate = issueDate;
            return this;
        }

        public TrialLicenseBuilder duration(TimeValue duration) {
            this.duration = duration;
            return this;
        }

        public TrialLicenseBuilder expiryDate(long expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public ESLicense build() {
            if (expiryDate == -1) {
                expiryDate = issueDate + duration.millis();
            }
            if (uid == null) {
                uid = UUID.randomUUID().toString();
            }
            return ESLicense.builder()
                    .type(DEFAULT_TYPE)
                    .subscriptionType(DEFAULT_SUBSCRIPTION_TYPE)
                    .issuer(DEFAULT_ISSUER)
                    .uid(uid)
                    .issuedTo(issuedTo)
                    .issueDate(issueDate)
                    .feature(feature)
                    .maxNodes(maxNodes)
                    .expiryDate(expiryDate)
                    .build();
        }

    }

    public static Set<ESLicense> fromEncodedTrialLicenses(Set<String> encodedTrialLicenses) {
        Set<ESLicense> licenses = new HashSet<>(encodedTrialLicenses.size());
        for (String encodedTrialLicense : encodedTrialLicenses) {
            licenses.add(fromEncodedTrialLicense(encodedTrialLicense));
        }
        return ImmutableSet.copyOf(licenses);
    }

    public static ESLicense fromEncodedTrialLicense(String encodedTrialLicense) {
        byte[] encodedBytes = Base64.decodeBase64(encodedTrialLicense);
        ByteBuffer byteBuffer = ByteBuffer.wrap(encodedBytes);

        int uidLen = byteBuffer.getInt();
        byte[] uidBytes = new byte[uidLen];
        byteBuffer.get(uidBytes);
        String uid = new String(uidBytes, StandardCharsets.UTF_8);

        int issuedToLen = byteBuffer.getInt();
        byte[] issuedToBytes = new byte[issuedToLen];
        byteBuffer.get(issuedToBytes);
        String issuedTo = new String(issuedToBytes, StandardCharsets.UTF_8);

        int featureLen = byteBuffer.getInt();
        byte[] featureBytes = new byte[featureLen];
        byteBuffer.get(featureBytes);
        String feature = new String(featureBytes, StandardCharsets.UTF_8);

        int maxNodes = byteBuffer.getInt();
        long issueDate = byteBuffer.getLong();
        long expiryDate = byteBuffer.getLong();

        return builder()
                .uid(uid)
                .issuedTo(issuedTo)
                .feature(feature)
                .maxNodes(maxNodes)
                .issueDate(issueDate)
                .expiryDate(expiryDate)
                .build();
    }

    public static String toEncodedTrialLicense(ESLicense trialLicense) {
        byte[] uidBytes = trialLicense.uid().getBytes(StandardCharsets.UTF_8);
        byte[] featureBytes = trialLicense.feature().getBytes(StandardCharsets.UTF_8);
        byte[] issuedToBytes = trialLicense.issuedTo().getBytes(StandardCharsets.UTF_8);

        // uid len + uid bytes + issuedTo len + issuedTo bytes + feature bytes length + feature bytes + maxNodes + issueDate + expiryDate
        int len = 4 + uidBytes.length + 4 + issuedToBytes.length + 4 + featureBytes.length + 4 + 8 + 8;
        final byte[] encodedLicense = new byte[len];
        ByteBuffer byteBuffer = ByteBuffer.wrap(encodedLicense);

        byteBuffer.putInt(uidBytes.length);
        byteBuffer.put(uidBytes);

        byteBuffer.putInt(issuedToBytes.length);
        byteBuffer.put(issuedToBytes);

        byteBuffer.putInt(featureBytes.length);
        byteBuffer.put(featureBytes);

        byteBuffer.putInt(trialLicense.maxNodes());
        byteBuffer.putLong(trialLicense.issueDate());
        byteBuffer.putLong(trialLicense.expiryDate());

        return Base64.encodeBase64String(encodedLicense);
    }
}
