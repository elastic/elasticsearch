/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static org.elasticsearch.license.core.shaded.CryptUtils.decrypt;
import static org.elasticsearch.license.core.shaded.CryptUtils.encrypt;

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

        public License build() {
            if (expiryDate == -1) {
                expiryDate = issueDate + duration.millis();
            }
            if (uid == null) {
                uid = UUID.randomUUID().toString();
            }
            return License.builder()
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

    public static License fromEncodedTrialLicense(String encodedTrialLicense) throws IOException {
        byte[] data = decrypt(Base64.decode(encodedTrialLicense));
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(data);
        parser.nextToken();
        return License.builder().fromXContent(parser).build();
    }

    public static String toEncodedTrialLicense(License trialLicense) throws IOException {
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        // trial license is equivalent to a license spec (no signature)
        trialLicense.toXContent(contentBuilder, new ToXContent.MapParams(Collections.singletonMap(Licenses.LICENSE_SPEC_VIEW_MODE, "true")));
        return Base64.encodeBytes(encrypt(contentBuilder.bytes().toBytes()));
    }
}
