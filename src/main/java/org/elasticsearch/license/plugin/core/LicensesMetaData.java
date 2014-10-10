/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.license.core.ESLicenses.ESLicense;
import static org.elasticsearch.license.core.ESLicenses.FeatureType;
import static org.elasticsearch.license.plugin.action.Utils.*;
import static org.elasticsearch.license.plugin.core.TrialLicenses.TrialLicense;

/**
 * Contains metadata about registered licenses
 *
 * TODO: store only signatures rather than the whole licenses json in cluster state
 */
public class LicensesMetaData implements MetaData.Custom {

    public static final String TYPE = "licenses";

    public static final Factory FACTORY = new Factory();

    private final ImmutableMap<FeatureType, ESLicense> licensesMap;

    private final ImmutableMap<FeatureType, TrialLicense> trialLicensesMap;

    /**
     * Constructs new licenses metadata
     *
     * @param esLicenses list of esLicense
     */
    public LicensesMetaData(ESLicenses esLicenses, TrialLicenses trialLicenses) {
        this.licensesMap = map(esLicenses);
        this.trialLicensesMap = map(trialLicenses);
    }


    public ESLicenses getLicenses() {
        return new ESLicenses() {
            @Override
            public Collection<ESLicense> licenses() {
                return licensesMap.values();
            }

            @Override
            public Set<FeatureType> features() {
                return licensesMap.keySet();
            }

            @Override
            public ESLicense get(FeatureType featureType) {
                return licensesMap.get(featureType);
            }

            @Override
            public Iterator<ESLicense> iterator() {
                return licensesMap.values().iterator();
            }
        };
    }

    public TrialLicenses getTrialLicenses() {
        return new TrialLicenses() {
            @Override
            public Collection<TrialLicense> trialLicenses() {
                return trialLicensesMap.values();
            }

            @Override
            public TrialLicense getTrialLicense(FeatureType featureType) {
                return trialLicensesMap.get(featureType);
            }

            @Override
            public Iterator<TrialLicense> iterator() {
                return trialLicensesMap.values().iterator();
            }
        };
    }


    private static ImmutableMap<FeatureType, ESLicense> map(ESLicenses esLicenses) {
        final ImmutableMap.Builder<FeatureType, ESLicense> builder = ImmutableMap.builder();
        if (esLicenses != null) {
            for (ESLicense esLicense : esLicenses) {
                builder.put(esLicense.feature(), esLicense);
            }
        }
        return builder.build();
    }

    private static ImmutableMap<FeatureType, TrialLicense> map(TrialLicenses trialLicenses) {
        final ImmutableMap.Builder<FeatureType, TrialLicense> builder = ImmutableMap.builder();
        if (trialLicenses != null) {
            for (TrialLicense esLicense : trialLicenses) {
                builder.put(esLicense.feature(), esLicense);
            }
        }
        return builder.build();
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
                esLicenses = readGeneratedLicensesFrom(in);
                trialLicenses = readTrialLicensesFrom(in);
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
                writeGeneratedLicensesTo(licensesMetaData.getLicenses(), out);
                writeTrialLicensesTo(licensesMetaData.getTrialLicenses(), out);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public LicensesMetaData fromXContent(XContentParser parser) throws IOException {

            XContentParser.Token token;
            final LicenseBuilders.LicensesBuilder licensesBuilder = LicenseBuilders.licensesBuilder();
            final TrialLicensesBuilder trialLicensesBuilder = TrialLicensesBuilder.trialLicensesBuilder();
            String fieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                }
                if (fieldName != null) {
                    if (fieldName.equals(Fields.LICENSES)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            licensesBuilder.license(licenseFromMap(parser.map()));
                        }
                    }
                    if (fieldName.equals(Fields.TRIAL_LICENSES)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            trialLicensesBuilder.license(trialLicenseFromMap(parser.map()));
                        }
                    }
                }
            }
            return new LicensesMetaData(licensesBuilder.build(), trialLicensesBuilder.build());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toXContent(LicensesMetaData licensesMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.startArray(Fields.LICENSES);
            for (ESLicense license : licensesMetaData.getLicenses()) {
                builder.map(licenseAsMap(license));
            }
            builder.endArray();

            builder.startArray(Fields.TRIAL_LICENSES);
            for (TrialLicense license : licensesMetaData.getTrialLicenses()) {
                builder.map(trialLicenseAsMap(license));
            }
            builder.endArray();
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