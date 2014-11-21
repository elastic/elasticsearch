/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Contains metadata about registered licenses
 */
public class LicensesMetaData implements MetaData.Custom {

    public static final String TYPE = "licenses";

    public static final Factory FACTORY = new Factory();

    private final ImmutableList<License> signedLicenses;

    private final ImmutableList<License> trialLicenses;

    /**
     * Constructs new licenses metadata
     *
     * @param signedLicenses list of signed Licenses
     * @param trialLicenses set of encoded trial licenses
     */
    public LicensesMetaData(List<License> signedLicenses, List<License>  trialLicenses) {
        this.signedLicenses = ImmutableList.copyOf(signedLicenses);
        this.trialLicenses = ImmutableList.copyOf(trialLicenses);
    }

    public List<License> getSignedLicenses() {
        return signedLicenses;
    }

    public List<License> getTrialLicenses() {
        return trialLicenses;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LicensesMetaData that = (LicensesMetaData) obj;
        return signedLicenses.equals(that.signedLicenses)
                && trialLicenses.equals(that.trialLicenses);
    }

    @Override
    public int hashCode() {
        return signedLicenses.hashCode() + 31 * trialLicenses.hashCode();
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
            List<License> signedLicenses = Licenses.readFrom(in);
            int numTrialLicenses = in.readVInt();
            List<License> trialLicenses = new ArrayList<>(numTrialLicenses);
            for (int i = 0; i < numTrialLicenses; i++) {
                trialLicenses.add(TrialLicenseUtils.fromEncodedTrialLicense(in.readString()));
            }
            return new LicensesMetaData(signedLicenses, trialLicenses);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeTo(LicensesMetaData licensesMetaData, StreamOutput out) throws IOException {
            Licenses.writeTo(licensesMetaData.signedLicenses, out);
            out.writeVInt(licensesMetaData.trialLicenses.size());
            for (License trialLicense : licensesMetaData.trialLicenses) {
                out.writeString(TrialLicenseUtils.toEncodedTrialLicense(trialLicense));
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public LicensesMetaData fromXContent(XContentParser parser) throws IOException {
            List<License> trialLicenses = new ArrayList<>();
            List<License> signedLicenses = new ArrayList<>();
            XContentParser.Token token;
            while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                token = parser.nextToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.text();
                    if (fieldName != null) {
                        if (fieldName.equals(Fields.TRIAL_LICENSES)) {
                            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    if (parser.currentToken().isValue()) {
                                        trialLicenses.add(TrialLicenseUtils.fromEncodedTrialLicense(parser.text()));
                                    }
                                }
                            }
                        }
                        if (fieldName.equals(Fields.SIGNED_LICENCES)) {
                            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    License.Builder builder = License.builder().fromXContent(parser);
                                    signedLicenses.add(builder.build());
                                }
                            }
                        }
                    }
                }
            }
            return new LicensesMetaData(signedLicenses, trialLicenses);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toXContent(LicensesMetaData licensesMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startArray(Fields.TRIAL_LICENSES);
            for (License trailLicense : licensesMetaData.trialLicenses) {
                builder.value(TrialLicenseUtils.toEncodedTrialLicense(trailLicense));
            }
            builder.endArray();

            builder.startArray(Fields.SIGNED_LICENCES);
            for (License license : licensesMetaData.signedLicenses) {
                license.toXContent(builder, params);
            }
            builder.endArray();
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }

        private final static class Fields {
            private static final String SIGNED_LICENCES = "signed_licenses";
            private static final String TRIAL_LICENSES = "trial_licenses";
        }
    }
}