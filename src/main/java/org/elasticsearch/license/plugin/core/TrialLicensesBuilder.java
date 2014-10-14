/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.license.core.DateUtils;
import org.elasticsearch.license.core.ESLicenses;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import static org.elasticsearch.license.plugin.core.TrialLicenses.TrialLicense;

public class TrialLicensesBuilder {

    public static TrialLicenses EMPTY = trialLicensesBuilder().build();

    public static TrialLicensesBuilder trialLicensesBuilder() {
        return new TrialLicensesBuilder();
    }

    public static TrialLicenseBuilder trialLicenseBuilder() {
        return new TrialLicenseBuilder();
    }


    public static TrialLicense fromEncodedTrialLicense(String encodedTrialLicense) {
        byte[] encodedBytes = Base64.decodeBase64(encodedTrialLicense);
        ByteBuffer byteBuffer = ByteBuffer.wrap(encodedBytes);

        int uidLen = byteBuffer.getInt();
        byte[] uidBytes = new byte[uidLen];
        byteBuffer.get(uidBytes);
        String uid = new String(uidBytes, Charset.forName("UTF-8"));

        int issuedToLen = byteBuffer.getInt();
        byte[] issuedToBytes = new byte[issuedToLen];
        byteBuffer.get(issuedToBytes);
        String issuedTo = new String(issuedToBytes, Charset.forName("UTF-8"));

        int featureLen = byteBuffer.getInt();
        byte[] featureBytes = new byte[featureLen];
        byteBuffer.get(featureBytes);
        String feature = new String(featureBytes, Charset.forName("UTF-8"));

        int maxNodes = byteBuffer.getInt();
        long issueDate = byteBuffer.getLong();
        long expiryDate = byteBuffer.getLong();

        return trialLicenseBuilder()
                .uid(uid)
                .issuedTo(issuedTo)
                .feature(ESLicenses.FeatureType.fromString(feature))
                .maxNodes(maxNodes)
                .issueDate(issueDate)
                .expiryDate(expiryDate)
                .build();
    }

    public static String toEncodedTrialLicense(TrialLicense trialLicense) {
        byte[] uidBytes = trialLicense.uid().getBytes(Charset.forName("UTF-8"));
        String feature = trialLicense.feature().string();
        byte[] featureBytes = feature.getBytes(Charset.forName("UTF-8"));
        byte[] issuedToBytes = trialLicense.issuedTo().getBytes(Charset.forName("UTF-8"));

        // uid len + uid bytes + issuedTo len + issuedTo bytes + feature bytes length + feature bytes + maxNodes + issueDate + expiryDate
        int len = 4 + uidBytes.length + issuedToBytes.length + featureBytes.length + 4 + 8 + 8;
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

    public static TrialLicenses merge(TrialLicenses trialLicenses, TrialLicenses mergeTrialLicenses) {
        if (trialLicenses == null && mergeTrialLicenses == null) {
            throw new IllegalArgumentException("both licenses can not be null");
        } else if (trialLicenses == null) {
            return mergeTrialLicenses;
        } else if (mergeTrialLicenses == null) {
            return trialLicenses;
        } else {
            return trialLicensesBuilder()
                    .licenses(trialLicenses.trialLicenses())
                    .licenses(mergeTrialLicenses.trialLicenses())
                    .build();
        }
    }

    private final ImmutableMap.Builder<ESLicenses.FeatureType, TrialLicense> licenseBuilder;

    public TrialLicensesBuilder() {
        licenseBuilder = ImmutableMap.builder();
    }

    public TrialLicensesBuilder license(TrialLicense trialLicense) {
        licenseBuilder.put(trialLicense.feature(), trialLicense);
        return this;
    }

    public TrialLicensesBuilder licenses(TrialLicenses trialLicenses) {
        return licenses(trialLicenses.trialLicenses());
    }

    public TrialLicensesBuilder licenses(Collection<TrialLicense> trialLicenses) {
        for (TrialLicense trialLicense : trialLicenses) {
            license(trialLicense);
        }
        return this;
    }

    public TrialLicenses build() {
        final ImmutableMap<ESLicenses.FeatureType, TrialLicense> licenseMap = licenseBuilder.build();
        return new TrialLicenses() {

            @Override
            public Collection<TrialLicense> trialLicenses() {
                return licenseMap.values();
            }

            @Override
            public TrialLicense getTrialLicense(ESLicenses.FeatureType featureType) {
                return licenseMap.get(featureType);
            }

            @Override
            public Iterator<TrialLicense> iterator() {
                return licenseMap.values().iterator();
            }
        };
    }

    public static class TrialLicenseBuilder {
        private ESLicenses.FeatureType featureType;
        private long expiryDate = -1;
        private long issueDate = -1;
        private int durationInDays = -1;
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

        public TrialLicenseBuilder feature(ESLicenses.FeatureType featureType) {
            this.featureType = featureType;
            return this;
        }

        public TrialLicenseBuilder issueDate(long issueDate) {
            this.issueDate = issueDate;
            return this;
        }

        public TrialLicenseBuilder durationInDays(int days) {
            this.durationInDays = days;
            return this;
        }

        public TrialLicenseBuilder expiryDate(long expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public TrialLicense build() {
            verify();
            if (expiryDate == -1) {
                assert durationInDays != -1;
                expiryDate = DateUtils.expiryDateAfterDays(issueDate, durationInDays);
            }
            if (uid == null) {
                uid = UUID.randomUUID().toString();
            }
            return new TrialLicense() {

                @Override
                public String issuedTo() {
                    return issuedTo;
                }

                @Override
                public ESLicenses.FeatureType feature() {
                    return featureType;
                }

                @Override
                public long issueDate() {
                    return issueDate;
                }

                @Override
                public long expiryDate() {
                    return expiryDate;
                }

                @Override
                public int maxNodes() {
                    return maxNodes;
                }

                @Override
                public String uid() {
                    return uid;
                }
            };
        }

        private void verify() {
            String msg = null;
            if (issuedTo == null) {
                msg = "issuedTo has to be set";
            } else if (featureType == null) {
                msg = "feature has to be set";
            } else if (issueDate == -1) {
                msg = "issueDate has to be set";
            } else if (durationInDays == -1 && expiryDate == -1) {
                msg = "durationInDays or expiryDate has to be set";
            } else if (maxNodes == -1) {
                msg = "maxNodes has to be set";
            }
            if (msg != null) {
                throw new IllegalArgumentException(msg);
            }
        }
    }
}
