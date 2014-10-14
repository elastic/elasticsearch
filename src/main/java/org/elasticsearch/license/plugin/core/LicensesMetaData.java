/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.core.ESLicenses;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.license.plugin.action.Utils.*;

/**
 * Contains metadata about registered licenses
 *
 */
public class LicensesMetaData implements MetaData.Custom {

    public static final String TYPE = "licenses";

    public static final Factory FACTORY = new Factory();

    private final ESLicenses licenses;

    private final TrialLicenses trialLicenses;

    /**
     * Constructs new licenses metadata
     *
     * @param esLicenses list of esLicense
     */
    public LicensesMetaData(ESLicenses esLicenses, TrialLicenses trialLicenses) {
        this.licenses = esLicenses;
        this.trialLicenses = trialLicenses;
    }


    public ESLicenses getLicenses() {
        return licenses;
    }

    public TrialLicenses getTrialLicenses() {
        return trialLicenses;
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
            ESLicenses esLicenses = null;
            TrialLicenses trialLicenses = null;
            if (in.readBoolean()) {
                esLicenses = readGeneratedLicensesFromMetaData(in);
                trialLicenses = readTrialLicensesFromMetaData(in);
            }
            return new LicensesMetaData(esLicenses, trialLicenses);
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
                writeGeneratedLicensesToMetaData(licensesMetaData.getLicenses(), out);
                writeTrialLicensesToMetaData(licensesMetaData.getTrialLicenses(), out);
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
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            signatures.add(parser.text());
                        }
                    }
                    if (fieldName.equals(Fields.TRIAL_LICENSES)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            encodedTrialLicenses.add(parser.text());
                        }
                    }
                }
            }

            return new LicensesMetaData(fromSignatures(signatures), fromEncodedTrialLicenses(encodedTrialLicenses));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toXContent(LicensesMetaData licensesMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.array(Fields.LICENSES, toSignatures(licensesMetaData.getLicenses()));
            builder.array(Fields.TRIAL_LICENSES, toEncodedTrialLicenses(licensesMetaData.getTrialLicenses()));
            builder.endObject();
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return MetaData.API_ONLY;
        }


        private final static class Fields {
            private static final String LICENSES = "licenses";
            private static final String TRIAL_LICENSES = "trial_licenses";
        }


    }
}