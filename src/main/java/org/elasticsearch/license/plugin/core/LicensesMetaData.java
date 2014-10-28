/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Contains metadata about registered licenses
 *
 */
public class LicensesMetaData implements MetaData.Custom {

    public static final String TYPE = "licenses";

    public static final Factory FACTORY = new Factory();

    private final Set<String> signatures;

    private final Set<String> encodedTrialLicenses;

    public LicensesMetaData(String[] signatures, String[] encodedTrialLicenses) {
        this(Sets.newHashSet(signatures), Sets.newHashSet(encodedTrialLicenses));
    }

    /**
     * Constructs new licenses metadata
     *
     * @param signatures set of esLicense signatures
     * @param encodedTrialLicenses set of encoded trial licenses
     */
    public LicensesMetaData(Set<String> signatures, Set<String> encodedTrialLicenses) {
        this.signatures = signatures;
        this.encodedTrialLicenses = encodedTrialLicenses;
    }

    public Set<String> getSignatures() {
        return signatures;
    }

    public Set<String> getEncodedTrialLicenses() {
        return encodedTrialLicenses;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof LicensesMetaData) {
            LicensesMetaData other = (LicensesMetaData) obj;
            boolean signaturesEqual;
            boolean trialLicensesEqual;

            if (other.getSignatures() != null) {
                if (this.getSignatures() != null) {
                    signaturesEqual = other.getSignatures().equals(this.getSignatures());
                } else {
                    return false;
                }
            } else {
                signaturesEqual = this.getSignatures() == null;
            }

            if (other.getEncodedTrialLicenses() != null) {
                if (this.getEncodedTrialLicenses() != null) {
                    trialLicensesEqual = other.getEncodedTrialLicenses().equals(this.getEncodedTrialLicenses());
                } else {
                    return false;
                }
            } else {
                trialLicensesEqual = this.getEncodedTrialLicenses() == null;
            }

            return signaturesEqual && trialLicensesEqual;
        }
        return false;
    }

    /**
     * Licenses metadata factory
     */
    public static class Factory extends MetaData.Custom.Factory<LicensesMetaData> {

        /**
         * {@inheritDoc}
         */
        @Override
        public String type() {
            return TYPE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public LicensesMetaData readFrom(StreamInput in) throws IOException {
            String[] signatures = new String[0];
            String[] encodedTrialLicenses = new String[0];
            if (in.readBoolean()) {
                signatures = in.readStringArray();
                encodedTrialLicenses = in.readStringArray();
            }
            return new LicensesMetaData(signatures, encodedTrialLicenses);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeTo(LicensesMetaData licensesMetaData, StreamOutput out) throws IOException {
            if (licensesMetaData == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeStringArray(licensesMetaData.signatures.toArray(new String[licensesMetaData.signatures.size()]));
                out.writeStringArray(licensesMetaData.encodedTrialLicenses.toArray(new String[licensesMetaData.encodedTrialLicenses.size()]));
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public LicensesMetaData fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token;
            String fieldName = null;
            Set<String> encodedTrialLicenses = new HashSet<>();
            Set<String> signatures = new HashSet<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                }
                if (fieldName != null) {
                    if (fieldName.equals(Fields.LICENSES)) {
                        if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                if (parser.currentToken().isValue()) {
                                    signatures.add(parser.text());
                                }
                            }
                        }
                    } else if (fieldName.equals(Fields.TRIAL_LICENSES)) {
                        if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                if (parser.currentToken().isValue()) {
                                    encodedTrialLicenses.add(parser.text());
                                }
                            }
                        }
                    }
                }
            }

            return new LicensesMetaData(signatures, encodedTrialLicenses);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toXContent(LicensesMetaData licensesMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.array(Fields.LICENSES, licensesMetaData.signatures.toArray(new String[licensesMetaData.signatures.size()]));
            builder.array(Fields.TRIAL_LICENSES, licensesMetaData.encodedTrialLicenses.toArray(new String [licensesMetaData.encodedTrialLicenses.size()]));
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }


        private final static class Fields {
            private static final String LICENSES = "licenses";
            private static final String TRIAL_LICENSES = "trial_licenses";
        }


    }
}