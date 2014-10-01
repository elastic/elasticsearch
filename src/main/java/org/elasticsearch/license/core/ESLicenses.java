/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import java.util.Collection;
import java.util.Set;


/**
 * Interface for ESLicenses, ESLicense
 * and enums for Type, SubscriptionType and FeatureType.
 * <p/>
 * This is the main contract between the licensor and the license manager
 */
public interface ESLicenses extends Iterable<ESLicenses.ESLicense> {

    /**
     * @return list of licenses contained under this instance
     */
    public Collection<ESLicense> licenses();

    /**
     * @return Set of features for which there exists an underlying license
     */
    public Set<FeatureType> features();

    /**
     * @return a license for a code>featureType<</code>
     */
    public ESLicense get(FeatureType featureType);

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

    /**
     * Enum for License FeatureType
     */
    public enum FeatureType {
        SHIELD("shield"),
        MARVEL("marvel");

        private final String name;

        private FeatureType(String name) {
            this.name = name;
        }

        public String string() {
            return name;
        }

        public static FeatureType fromString(String featureType) {
            if (featureType.equalsIgnoreCase(SHIELD.string())) {
                return SHIELD;
            } else if (featureType.equalsIgnoreCase(MARVEL.string())) {
                return MARVEL;
            } else {
                throw new IllegalArgumentException("Invalid FeatureType=" + featureType);
            }
        }
    }

    /**
     * Interface representing all the license fields
     */
    public interface ESLicense {

        /**
         * @return a unique identifier for a license (currently just a UUID)
         */
        public String uid();

        /**
         * @return type of the license [trial, subscription, internal]
         */
        public Type type();

        /**
         * @return subscription type of the license [none, silver, gold, platinum]
         */
        public SubscriptionType subscriptionType();

        /**
         * @return the issueDate in milliseconds
         */
        public long issueDate();

        /**
         * @return the featureType for the license [shield, marvel]
         */
        public FeatureType feature();

        /**
         * @return the expiry date in milliseconds
         */
        public long expiryDate();

        /**
         * @return the maximum number of nodes this license has been issued for
         */
        public int maxNodes();

        /**
         * @return a string representing the entity this licenses has been issued to
         */
        public String issuedTo();

        /**
         * @return a string representing the entity responsible for issuing this license (internal)
         */
        public String issuer();

        /**
         * @return a string representing the signature of the license used for license verification
         */
        public String signature();
    }

}
