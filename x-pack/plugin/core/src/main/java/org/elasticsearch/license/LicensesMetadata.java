/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

/**
 * Contains metadata about registered licenses
 */
public class LicensesMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "licenses";

    /**
     * When license is explicitly removed by a user, LICENSE_TOMBSTONE
     * is used as a placeholder in the license metadata. This enables
     * us to distinguish between the scenario when a cluster never
     * had a license (null) and when a license was removed explicitly
     * (LICENSE_TOMBSTONE).
     * We rely on this to decide whether to generate a unsigned trial
     * license or not. we should only generate a license if no license
     * ever existed in the cluster state
     */
    public static final License LICENSE_TOMBSTONE = License.builder()
        .type(License.LicenseType.TRIAL)
        .issuer("elasticsearch")
        .uid("TOMBSTONE")
        .issuedTo("")
        .maxNodes(0)
        .issueDate(0)
        .expiryDate(0)
        .build();

    private final License license;

    // This field describes the version of x-pack for which this cluster has exercised a trial. If the field
    // is null, then no trial has been exercised. We keep the version to leave open the possibility that we
    // may eventually allow a cluster to exercise a trial every time they upgrade to a new major version.
    @Nullable
    private Version trialVersion;

    public LicensesMetadata(License license, Version trialVersion) {
        this.license = license;
        this.trialVersion = trialVersion;
    }

    public License getLicense() {
        return license;
    }

    boolean isEligibleForTrial() {
        if (trialVersion == null) {
            return true;
        }
        return Version.CURRENT.major > trialVersion.major;
    }

    Version getMostRecentTrialVersion() {
        return trialVersion;
    }

    @Override
    public String toString() {
        return "LicensesMetadata{" + "license=" + license + ", trialVersion=" + trialVersion + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LicensesMetadata that = (LicensesMetadata) o;

        return Objects.equals(license, that.license) && Objects.equals(trialVersion, that.trialVersion);
    }

    @Override
    public int hashCode() {
        int result = license != null ? license.hashCode() : 0;
        result = 31 * result + (trialVersion != null ? trialVersion.hashCode() : 0);
        return result;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.MINIMUM_COMPATIBLE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY);
    }

    public static LicensesMetadata fromXContent(XContentParser parser) throws IOException {
        License license = LICENSE_TOMBSTONE;
        Version trialLicense = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                if (fieldName != null) {
                    if (fieldName.equals(Fields.LICENSE)) {
                        token = parser.nextToken();
                        if (token == XContentParser.Token.START_OBJECT) {
                            license = License.fromXContent(parser);
                        } else if (token == XContentParser.Token.VALUE_NULL) {
                            license = LICENSE_TOMBSTONE;
                        }
                    } else if (fieldName.equals(Fields.TRIAL_LICENSE)) {
                        parser.nextToken();
                        trialLicense = Version.fromString(parser.text());
                    }
                }
            }
        }
        return new LicensesMetadata(license, trialLicense);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.single(((builder, params) -> {
            if (license == LICENSE_TOMBSTONE) {
                builder.nullField(Fields.LICENSE);
            } else {
                builder.startObject(Fields.LICENSE);
                license.toInnerXContent(builder, params);
                builder.endObject();
            }
            if (trialVersion != null) {
                builder.field(Fields.TRIAL_LICENSE, trialVersion.toString());
            }
            return builder;
        }));
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        if (license == LICENSE_TOMBSTONE) {
            streamOutput.writeBoolean(false); // no license
        } else {
            streamOutput.writeBoolean(true); // has a license
            license.writeTo(streamOutput);
        }
        if (trialVersion == null) {
            streamOutput.writeBoolean(false);
        } else {
            streamOutput.writeBoolean(true);
            Version.writeVersion(trialVersion, streamOutput);
        }
    }

    public LicensesMetadata(StreamInput streamInput) throws IOException {
        if (streamInput.readBoolean()) {
            license = License.readLicense(streamInput);
        } else {
            license = LICENSE_TOMBSTONE;
        }
        boolean hasExercisedTrial = streamInput.readBoolean();
        if (hasExercisedTrial) {
            this.trialVersion = Version.readVersion(streamInput);
        }
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput streamInput) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, streamInput);
    }

    public static License extractLicense(LicensesMetadata licensesMetadata) {
        if (licensesMetadata != null) {
            License license = licensesMetadata.getLicense();
            if (license == LicensesMetadata.LICENSE_TOMBSTONE) {
                return null;
            } else {
                return license;
            }
        }
        return null;
    }

    private static final class Fields {
        private static final String LICENSE = "license";
        private static final String TRIAL_LICENSE = "trial_license";
    }
}
