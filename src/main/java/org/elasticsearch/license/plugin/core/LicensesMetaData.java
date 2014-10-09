/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableMap;
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
 */
public class LicensesMetaData implements MetaData.Custom, ESLicenses {

    public static final String TYPE = "licenses";

    public static final Factory FACTORY = new Factory();

    private final ImmutableMap<FeatureType, ESLicense> licenses;

    /**
     * Constructs new licenses metadata
     *
     * @param esLicenses list of esLicense
     */
    public LicensesMetaData(List<ESLicense> esLicenses) {
        this.licenses = map(esLicenses);
    }

    public LicensesMetaData(ESLicenses esLicenses) {
        this.licenses = map(esLicenses);
    }

    private static ImmutableMap<FeatureType, ESLicense> map(Iterable<ESLicense> esLicenses) {
        final ImmutableMap.Builder<FeatureType, ESLicense> builder = ImmutableMap.builder();
        if (esLicenses != null) {
            for (ESLicense esLicense : esLicenses) {
                builder.put(esLicense.feature(), esLicense);
            }
        }
        return builder.build();
    }

    @Override
    public Collection<ESLicense> licenses() {
        return licenses.values();
    }

    @Override
    public Set<FeatureType> features() {
        return licenses.keySet();
    }

    @Override
    public ESLicense get(FeatureType featureType) {
        return licenses.get(featureType);
    }

    @Override
    public Iterator<ESLicense> iterator() {
        return licenses.values().iterator();
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
            return new LicensesMetaData(readLicensesFrom(in));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeTo(LicensesMetaData licensesMetaData, StreamOutput out) throws IOException {
            writeLicensesTo(licensesMetaData, out);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public LicensesMetaData fromXContent(XContentParser parser) throws IOException {

            XContentParser.Token token;
            List<ESLicense> licenses = null;
            String fieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                }
                if (fieldName != null && fieldName.equals(Fields.LICENSES)) {
                    if (licenses == null) {
                        licenses = new ArrayList<>();
                    }
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        licenses.add(licenseFromMap(parser.map()));
                    }
                }
            }
            if (licenses == null) {
                throw new ElasticsearchParseException("failed to parse licenses: expected ['" + Fields.LICENSES + "']");
            }
            return new LicensesMetaData(licenses);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toXContent(LicensesMetaData licensesMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.startArray(Fields.LICENSES);
            for (ESLicense license : licensesMetaData.licenses()) {
                builder.map(licenseAsMap(license));
            }
            builder.endArray();
            builder.endObject();
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.API);
        }


        private final static class Fields {
            private static final String LICENSES = "licenses";
        }


    }
}