/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.cluster.MergableCustomMetaData;

import java.io.IOException;
import java.util.EnumSet;

/**
 * Contains metadata about registered licenses
 */
class LicensesMetaData extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom,
        MergableCustomMetaData<LicensesMetaData> {

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
            .type("trial")
            .issuer("elasticsearch")
            .uid("TOMBSTONE")
            .issuedTo("")
            .maxNodes(0)
            .issueDate(0)
            .expiryDate(0)
            .build();

    private License license;

    LicensesMetaData(License license) {
        this.license = license;
    }

    public License getLicense() {
        return license;
    }

    @Override
    public String toString() {
        if (license != null) {
            return license.toString();
        }
        return "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LicensesMetaData that = (LicensesMetaData) o;
        return !(license != null ? !license.equals(that.license) : that.license != null);
    }

    @Override
    public int hashCode() {
        return license != null ? license.hashCode() : 0;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY);
    }

    public static LicensesMetaData fromXContent(XContentParser parser) throws IOException {
        License license = LICENSE_TOMBSTONE;
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
                    }
                }
            }
        }
        return new LicensesMetaData(license);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (license == LICENSE_TOMBSTONE) {
            builder.nullField(Fields.LICENSE);
        } else {
            builder.startObject(Fields.LICENSE);
            license.toInnerXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        if (license == LICENSE_TOMBSTONE) {
            streamOutput.writeBoolean(false); // no license
        } else {
            streamOutput.writeBoolean(true); // has a license
            license.writeTo(streamOutput);
        }
    }

    LicensesMetaData(StreamInput streamInput) throws IOException {
        if (streamInput.readBoolean()) {
            license = License.readLicense(streamInput);
        } else {
            license = LICENSE_TOMBSTONE;
        }
    }

    public static NamedDiff<MetaData.Custom> readDiffFrom(StreamInput streamInput) throws IOException {
        return readDiffFrom(MetaData.Custom.class, TYPE, streamInput);
    }

    @Override
    public LicensesMetaData merge(LicensesMetaData other) {
        if (other.license == null) {
            return this;
        } else if (license == null
                || OperationMode.compare(other.license.operationMode(), license.operationMode()) > 0) {
            return other;
        }
        return this;
    }

    private static final class Fields {
        private static final String LICENSE = "license";
    }
}
