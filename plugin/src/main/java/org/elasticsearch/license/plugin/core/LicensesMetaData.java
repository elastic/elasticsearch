/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

/**
 * Contains metadata about registered licenses
 */
public class LicensesMetaData extends AbstractDiffable<MetaData.Custom> implements MetaData.Custom {

    public static final String TYPE = "licenses";

    public static final LicensesMetaData PROTO = new LicensesMetaData(Collections.<License>emptyList(), Collections.<License>emptyList());

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

    @Override
    public String type() {
        return TYPE;
    }

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

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY);
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        Licenses.writeTo(signedLicenses, streamOutput);
        streamOutput.writeVInt(trialLicenses.size());
        for (License trialLicense : trialLicenses) {
            streamOutput.writeString(TrialLicenseUtils.toEncodedTrialLicense(trialLicense));
        }
    }

    @Override
    public MetaData.Custom readFrom(StreamInput streamInput) throws IOException {
        List<License> signedLicenses = Licenses.readFrom(streamInput);
        int numTrialLicenses = streamInput.readVInt();
        List<License> trialLicenses = new ArrayList<>(numTrialLicenses);
        for (int i = 0; i < numTrialLicenses; i++) {
            trialLicenses.add(TrialLicenseUtils.fromEncodedTrialLicense(streamInput.readString()));
        }
        return new LicensesMetaData(signedLicenses, trialLicenses);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.TRIAL_LICENSES);
        for (License trailLicense : trialLicenses) {
            builder.value(TrialLicenseUtils.toEncodedTrialLicense(trailLicense));
        }
        builder.endArray();

        builder.startArray(Fields.SIGNED_LICENCES);
        for (License license : signedLicenses) {
            license.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    private final static class Fields {
        private static final String SIGNED_LICENCES = "signed_licenses";
        private static final String TRIAL_LICENSES = "trial_licenses";
    }
}