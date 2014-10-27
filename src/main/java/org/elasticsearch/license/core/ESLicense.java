/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ESLicense implements Comparable<ESLicense> {

    private final String uid;
    private final String issuer;
    private final String issuedTo;
    private final long issueDate;
    private final Type type;
    private final SubscriptionType subscriptionType;
    private final String feature;
    private final String signature;
    private final long expiryDate;
    private final int maxNodes;

    private ESLicense(String uid, String issuer, String issuedTo, long issueDate, Type type,
                     SubscriptionType subscriptionType, String feature, String signature, long expiryDate, int maxNodes) {
        this.uid = uid;
        this.issuer = issuer;
        this.issuedTo = issuedTo;
        this.issueDate = issueDate;
        this.type = type;
        this.subscriptionType = subscriptionType;
        this.feature = feature;
        this.signature = signature;
        this.expiryDate = expiryDate;
        this.maxNodes = maxNodes;
    }


    /**
     * @return a unique identifier for a license (currently just a UUID)
     */
    public String uid() {
        return uid;
    }

    /**
     * @return type of the license [trial, subscription, internal]
     */
    public Type type() {
        return type;
    }

    /**
     * @return subscription type of the license [none, silver, gold, platinum]
     */
    public SubscriptionType subscriptionType() {
        return subscriptionType;
    }

    /**
     * @return the issueDate in milliseconds
     */
    public long issueDate() {
        return issueDate;
    }

    /**
     * @return the featureType for the license [shield, marvel]
     */
    public String feature() {
        return feature;
    }

    /**
     * @return the expiry date in milliseconds
     */
    public long expiryDate() {
        return expiryDate;
    }

    /**
     * @return the maximum number of nodes this license has been issued for
     */
    public int maxNodes() {
        return maxNodes;
    }

    /**
     * @return a string representing the entity this licenses has been issued to
     */
    public String issuedTo() {
        return issuedTo;
    }

    /**
     * @return a string representing the entity responsible for issuing this license (internal)
     */
    public String issuer() {
        return issuer;
    }

    /**
     * @return a string representing the signature of the license used for license verification
     */
    public String signature() {
        return signature;
    }

    @Override
    public int compareTo(ESLicense o) {
        assert o != null;
        return Long.compare(expiryDate, o.expiryDate);
    }

    /**
     * Enum for License Type
     */
    public enum Type {
        TRIAL("trial"),
        SUBSCRIPTION("subscription"),
        INTERNAL("internal");

        private final String name;

        private Type(String name) {
            this.name = name;
        }

        public String string() {
            return name;
        }

        public static Type fromString(String type) {
            if (type.equalsIgnoreCase(TRIAL.string())) {
                return TRIAL;
            } else if (type.equalsIgnoreCase(SUBSCRIPTION.string())) {
                return SUBSCRIPTION;
            } else if (type.equalsIgnoreCase(INTERNAL.string())) {
                return INTERNAL;
            } else {
                throw new IllegalArgumentException("Invalid Type=" + type);
            }

        }
    }

    /**
     * Enum for License Subscription Type
     */
    public enum SubscriptionType {
        NONE("none"),
        DEVELOPMENT("development"),
        SILVER("silver"),
        GOLD("gold"),
        PLATINUM("platinum");

        public static SubscriptionType DEFAULT = NONE;

        private final String name;

        private SubscriptionType(String name) {
            this.name = name;
        }

        public String string() {
            return name;
        }

        public static SubscriptionType fromString(String subscriptionType) {
            if (subscriptionType.equalsIgnoreCase(NONE.string())) {
                return NONE;
            } else if (subscriptionType.equalsIgnoreCase(DEVELOPMENT.string())) {
                return DEVELOPMENT;
            } else if (subscriptionType.equalsIgnoreCase(SILVER.string())) {
                return SILVER;
            } else if (subscriptionType.equalsIgnoreCase(GOLD.string())) {
                return GOLD;
            } else if (subscriptionType.equalsIgnoreCase(PLATINUM.string())) {
                return PLATINUM;
            } else {
                throw new IllegalArgumentException("Invalid SubscriptionType=" + subscriptionType);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String uid;
        private String issuer;
        private String issuedTo;
        private long issueDate = -1;
        private Type type;
        private SubscriptionType subscriptionType = SubscriptionType.DEFAULT;
        private String feature;
        private String signature;
        private long expiryDate = -1;
        private int maxNodes;


        public Builder uid(String uid) {
            this.uid = uid;
            return this;
        }

        public Builder issuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        public Builder issuedTo(String issuedTo) {
            this.issuedTo = issuedTo;
            return this;
        }

        public Builder issueDate(long issueDate) {
            this.issueDate = issueDate;
            return this;
        }

        public Builder type(Type type) {
            this.type = type;
            return this;
        }

        public Builder subscriptionType(SubscriptionType subscriptionType) {
            this.subscriptionType = subscriptionType;
            return this;
        }

        public Builder feature(String feature) {
            this.feature = feature;
            return this;
        }

        public Builder expiryDate(long expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public Builder maxNodes(int maxNodes) {
            this.maxNodes = maxNodes;
            return this;
        }

        public Builder signature(String signature) {
            if (signature != null) {
                this.signature = signature;
            }
            return this;
        }

        public Builder fromLicenseSpec(ESLicense license, String signature) {
            return uid(license.uid())
                    .issuedTo(license.issuedTo())
                    .issueDate(license.issueDate())
                    .type(license.type())
                    .subscriptionType(license.subscriptionType())
                    .feature(license.feature())
                    .maxNodes(license.maxNodes())
                    .expiryDate(license.expiryDate())
                    .issuer(license.issuer())
                    .signature(signature);
        }

        public ESLicense verifyAndBuild() {
            verify();
            return new ESLicense(uid, issuer, issuedTo, issueDate, type,
                    subscriptionType, feature, signature, expiryDate, maxNodes);
        }

        public ESLicense build() {
            return new ESLicense(uid, issuer, issuedTo, issueDate, type,
                    subscriptionType, feature, signature, expiryDate, maxNodes);
        }

        private void verify() {
            if (issuer == null) {
               throw new IllegalStateException("issuer can not be null");
            } else if (issuedTo == null) {
               throw new IllegalStateException("issuedTo can not be null");
            } else if (issueDate == -1) {
               throw new IllegalStateException("issueDate has to be set");
            } else if (type == null) {
               throw new IllegalStateException("type can not be null");
            } else if (subscriptionType == null) {
               throw new IllegalStateException("subscriptionType can not be null");
            } else if (uid == null) {
               throw new IllegalStateException("uid can not be null");
            } else if (feature == null) {
               throw new IllegalStateException("at least one feature has to be enabled");
            } else if (signature == null) {
               throw new IllegalStateException("signature can not be null");
            } else if (maxNodes == -1) {
               throw new IllegalStateException("maxNodes has to be set");
            } else if (expiryDate == -1) {
               throw new IllegalStateException("expiryDate has to be set");
            }
        }
    }


    final static class Fields {
        static final String UID = "uid";
        static final String TYPE = "type";
        static final String SUBSCRIPTION_TYPE = "subscription_type";
        static final String ISSUE_DATE = "issue_date";
        static final String FEATURE = "feature";
        static final String EXPIRY_DATE = "expiry_date";
        static final String MAX_NODES = "max_nodes";
        static final String ISSUED_TO = "issued_to";
        static final String ISSUER = "issuer";
        static final String SIGNATURE = "signature";
    }


    static void toXContent(ESLicense license, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(Fields.UID, license.uid);
        builder.field(Fields.TYPE, license.type.string());
        builder.field(Fields.SUBSCRIPTION_TYPE, license.subscriptionType.string());
        builder.field(Fields.ISSUE_DATE, license.issueDate);
        builder.field(Fields.FEATURE, license.feature);
        builder.field(Fields.EXPIRY_DATE, license.expiryDate);
        builder.field(Fields.MAX_NODES, license.maxNodes);
        builder.field(Fields.ISSUED_TO, license.issuedTo);
        builder.field(Fields.ISSUER, license.issuer);
        builder.field(Fields.SIGNATURE, license.signature);
        builder.endObject();
    }


    static ESLicense fromXContent(Map<String, Object> map) throws IOException {
        return new Builder()
                .uid((String) map.get(Fields.UID))
                .type(Type.fromString((String) map.get(Fields.TYPE)))
                .subscriptionType(SubscriptionType.fromString((String) map.get(Fields.SUBSCRIPTION_TYPE)))
                .feature((String) map.get(Fields.FEATURE))
                .maxNodes((int) map.get(Fields.MAX_NODES))
                .issuedTo((String) map.get(Fields.ISSUED_TO))
                .signature((String) map.get(Fields.SIGNATURE))
                .issueDate((long) map.get(Fields.ISSUE_DATE))
                .expiryDate((long) map.get(Fields.EXPIRY_DATE))
                .issuer((String) map.get(Fields.ISSUER))
                .verifyAndBuild();
    }

    static ESLicense readFrom(StreamInput in) throws IOException {
        Map<String, Object> licenseMap = in.readMap();
        return builder()
                .uid((String) licenseMap.get(Fields.UID))
                .type(Type.fromString((String) licenseMap.get(Fields.TYPE)))
                .subscriptionType(SubscriptionType.fromString((String) licenseMap.get(Fields.SUBSCRIPTION_TYPE)))
                .issueDate((long) licenseMap.get(Fields.ISSUE_DATE))
                .feature((String) licenseMap.get(Fields.FEATURE))
                .expiryDate((long) licenseMap.get(Fields.EXPIRY_DATE))
                .maxNodes((int) licenseMap.get(Fields.MAX_NODES))
                .issuedTo((String) licenseMap.get(Fields.ISSUED_TO))
                .signature((String) licenseMap.get(Fields.SIGNATURE))
                .issuer((String) licenseMap.get(Fields.ISSUER))
                .verifyAndBuild();
    }

    static void writeTo(ESLicense esLicense, StreamOutput out) throws IOException {
        Map<String, Object> licenseMap = new HashMap<>();
        licenseMap.put(Fields.UID, esLicense.uid);
        licenseMap.put(Fields.TYPE, esLicense.type.string());
        licenseMap.put(Fields.SUBSCRIPTION_TYPE, esLicense.subscriptionType.string());
        licenseMap.put(Fields.ISSUE_DATE, esLicense.issueDate);
        licenseMap.put(Fields.FEATURE, esLicense.feature);
        licenseMap.put(Fields.EXPIRY_DATE, esLicense.expiryDate);
        licenseMap.put(Fields.MAX_NODES, esLicense.maxNodes);
        licenseMap.put(Fields.ISSUED_TO, esLicense.issuedTo);
        licenseMap.put(Fields.ISSUER, esLicense.issuer);
        licenseMap.put(Fields.SIGNATURE, esLicense.signature);
        out.writeMap(licenseMap);
    }

}
