/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import java.util.*;

import static org.elasticsearch.license.core.ESLicenses.*;

public class LicenseBuilders {

    /**
     * @return a licenses builder instance to build a {@link org.elasticsearch.license.core.ESLicenses}
     */
    public static LicensesBuilder licensesBuilder() {
        return new LicensesBuilder();
    }

    /**
     * @return a license builder instance to build a {@link org.elasticsearch.license.core.ESLicenses.ESLicense}
     * if internal is set to true, then license fields (which are internal) are required to be set
     */
    public static LicenseBuilder licenseBuilder(boolean internal) {
        return new LicenseBuilder(internal);
    }

    /**
     * Merges all the sub-licenses of the provided licenses parameters by
     * longest expiry date for each license feature and merges out any
     * sub-licenses that have already expired
     *
     * @return a merged <code>ESLicenses</code> instance from <code>licenses</code>
     * and <code>mergedLicenses</code>
     */
    public static ESLicenses merge(ESLicenses licenses, ESLicenses mergeLicenses) {
        if (licenses == null && mergeLicenses == null) {
            throw new IllegalArgumentException("both licenses can not be null");
        } else if (licenses == null) {
            return mergeLicenses;
        } else if (mergeLicenses == null) {
            return licenses;
        } else {
            return licensesBuilder()
                    .licenses(licenses)
                    .licenses(mergeLicenses)
                    .build();
        }
    }

    public static ESLicenses removeFeatures(ESLicenses licenses, Set<String> featureTypesToDelete) {
        final LicensesBuilder licensesBuilder = licensesBuilder();
        for (ESLicense license : licenses) {
            if (!featureTypesToDelete.contains(license.feature())) {
                licensesBuilder.licenseAsIs(license);
            }
        }
        return licensesBuilder.build();
    }

    public static class LicensesBuilder {
        private Map<String, ESLicense> licenseMap = new HashMap<>();

        public LicensesBuilder() {
        }

        public LicensesBuilder license(LicenseBuilder builder) {
            return license(builder.build());
        }

        public LicensesBuilder license(ESLicense license) {
            putIfAppropriate(license);
            return this;
        }

        public LicensesBuilder licenseAsIs(ESLicense license) {
            licenseMap.put(license.feature(), license);
            return this;
        }

        public LicensesBuilder licenses(Collection<ESLicense> licenses) {
            for (ESLicense esLicense : licenses) {
                license(esLicense);
            }
            return this;
        }

        public LicensesBuilder licenses(ESLicenses licenses) {
            return licenses(licenses.licenses());
        }

        public ESLicenses build() {
            return new ESLicenses() {
                @Override
                public Collection<ESLicense> licenses() {
                    return licenseMap.values();
                }

                @Override
                public Set<String> features() {
                    return licenseMap.keySet();
                }

                @Override
                public ESLicense get(String feature) {
                    return licenseMap.get(feature);
                }

                @Override
                public Iterator<ESLicense> iterator() {
                    return licenseMap.values().iterator();
                }
            };
        }

        /**
         * Add a {@link org.elasticsearch.license.core.ESLicenses.ESLicense} to
         * {@link org.elasticsearch.license.core.ESLicenses} only if
         * there exists no License for the feature that has a longer expiry date
         * and if the license in question has an <code>expiryDate</code> that has
         * not expired yet
         *
         * @param license license in question
         */
        private void putIfAppropriate(ESLicense license) {
            final String featureType = license.feature();
            if (licenseMap.containsKey(featureType)) {
                final ESLicense previousLicense = licenseMap.get(featureType);
                if (license.expiryDate() > previousLicense.expiryDate()) {
                    licenseMap.put(featureType, license);
                }
            } else if (license.expiryDate() > System.currentTimeMillis()) {
                licenseMap.put(featureType, license);
            }
        }
    }

    public static class LicenseBuilder {
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


        private final boolean internal;

        public LicenseBuilder(boolean internal) {
            this.internal = internal;
        }

        public LicenseBuilder uid(String uid) {
            this.uid = uid;
            return this;
        }

        public LicenseBuilder issuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        public LicenseBuilder issuedTo(String issuedTo) {
            this.issuedTo = issuedTo;
            return this;
        }

        public LicenseBuilder issueDate(long issueDate) {
            this.issueDate = issueDate;
            return this;
        }

        public LicenseBuilder type(Type type) {
            this.type = type;
            return this;
        }

        public LicenseBuilder subscriptionType(SubscriptionType subscriptionType) {
            this.subscriptionType = subscriptionType;
            return this;
        }

        public LicenseBuilder feature(String feature) {
            this.feature = feature;
            return this;
        }

        public LicenseBuilder expiryDate(long expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public LicenseBuilder maxNodes(int maxNodes) {
            this.maxNodes = maxNodes;
            return this;
        }

        public LicenseBuilder signature(String signature) {
            if (signature != null) {
                this.signature = signature;
            }
            return this;
        }

        public LicenseBuilder fromLicense(ESLicense license) {
            LicenseBuilder builder = this.uid(license.uid())
                    .issuedTo(license.issuedTo())
                    .issueDate(license.issueDate())
                    .type(license.type())
                    .subscriptionType(license.subscriptionType())
                    .feature(license.feature())
                    .maxNodes(license.maxNodes())
                    .expiryDate(license.expiryDate());

            return (internal)
                    ? builder.issuer(license.issuer()).signature(license.signature())
                    : builder;


        }

        public ESLicense build() {
            if (uid == null) {
                uid = UUID.randomUUID().toString();
            }
            verify();
            return new ESLicense() {
                @Override
                public String uid() {
                    return uid;
                }

                @Override
                public Type type() {
                    return type;
                }

                @Override
                public SubscriptionType subscriptionType() {
                    return subscriptionType;
                }

                @Override
                public long issueDate() {
                    return issueDate;
                }

                @Override
                public String feature() {
                    return feature;
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
                public String issuer() {
                    return issuer;
                }

                @Override
                public String issuedTo() {
                    return issuedTo;
                }

                @Override
                public String signature() {
                    return signature;
                }
            };
        }

        private void verify() {
            String msg = null;
            if (internal && issuer == null) {
                msg = "issuer can not be null";
            } else if (issuedTo == null) {
                msg = "issuedTo can not be null";
            } else if (issueDate == -1) {
                msg = "issueDate has to be set";
            } else if (type == null) {
                msg = "type can not be null";
            } else if (subscriptionType == null) {
                msg = "subscriptionType can not be null";
            } else if (uid == null) {
                msg = "uid can not be null";
            } else if (feature == null) {
                msg = "at least one feature has to be enabled";
            } else if (internal && signature == null) {
                msg = "signature can not be null";
            } else if (maxNodes == -1) {
                msg = "maxNodes has to be set";
            } else if (expiryDate == -1) {
                msg = "expiryDate has to be set";
            }

            if (msg != null) {
                throw new IllegalStateException(msg);
            }
        }
    }


}
