/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.License;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.license.core.CryptUtils.decrypt;
import static org.elasticsearch.license.core.CryptUtils.encrypt;

/**
 * Contains metadata about registered licenses
 */
public class LicensesMetaData extends AbstractDiffable<MetaData.Custom> implements MetaData.Custom {

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

    public static final LicensesMetaData PROTO = new LicensesMetaData(null);

    private License license;

    public LicensesMetaData(License license) {
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
    public String type() {
        return TYPE;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY);
    }

    @Override
    public LicensesMetaData fromXContent(XContentParser parser) throws IOException {
        List<License> pre20Licenses = new ArrayList<>(1);
        License license = LICENSE_TOMBSTONE;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                if (fieldName != null) {
                    // for back compat with 1.x license metadata
                    if (fieldName.equals(Fields.TRIAL_LICENSES) || fieldName.equals(Fields.SIGNED_LICENCES)) {
                        token = parser.nextToken();
                        if (token == XContentParser.Token.START_ARRAY) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                if (parser.currentToken().isValue()) {
                                    // trial license
                                    byte[] data = decrypt(Base64.decode(parser.text()));
                                    try (XContentParser trialLicenseParser =
                                                 XContentFactory.xContent(XContentType.JSON).createParser(data)) {
                                        trialLicenseParser.nextToken();
                                        License pre20TrialLicense = License.fromXContent(trialLicenseParser);
                                        pre20Licenses.add(TrialLicense.create(License.builder().fromPre20LicenseSpec(pre20TrialLicense)));
                                    }
                                } else {
                                    // signed license
                                    pre20Licenses.add(License.fromXContent(parser));
                                }
                            }
                        }
                    } else if (fieldName.equals(Fields.LICENSE)) {
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
        // when we see old license metadata,
        // we try to choose the license that has the latest issue date that is not expired
        if (!pre20Licenses.isEmpty()) {
            // take the best unexpired license
            CollectionUtil.timSort(pre20Licenses, License.LATEST_ISSUE_DATE_FIRST);
            long now = System.currentTimeMillis();
            for (License oldLicense : pre20Licenses) {
                if (oldLicense.expiryDate() > now) {
                    license = oldLicense;
                    break;
                }
            }
            // take the best expired license
            if (license == LICENSE_TOMBSTONE && !pre20Licenses.isEmpty()) {
                license = pre20Licenses.get(0);
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
        if (streamOutput.getVersion().before(Version.V_2_0_0)) {
            if (license == LICENSE_TOMBSTONE) {
                streamOutput.writeVInt(0); // no signed license
                streamOutput.writeVInt(0); // no trial license
            } else if (!License.isAutoGeneratedLicense(license.signature())) {
                streamOutput.writeVInt(1); // one signed license
                license.writeTo(streamOutput);
                streamOutput.writeVInt(0); // no trial license
            } else {
                streamOutput.writeVInt(0); // no signed license
                streamOutput.writeVInt(1); // one trial license
                XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
                license.toXContent(contentBuilder,
                        new ToXContent.MapParams(Collections.singletonMap(License.LICENSE_SPEC_VIEW_MODE, "true")));
                streamOutput.writeString(Base64.encodeBytes(encrypt(contentBuilder.bytes().toBytes())));
            }
        } else {
            if (license == LICENSE_TOMBSTONE) {
                streamOutput.writeBoolean(false); // no license
            } else {
                streamOutput.writeBoolean(true); // has a license
                license.writeTo(streamOutput);
            }
        }
    }

    @Override
    public LicensesMetaData readFrom(StreamInput streamInput) throws IOException {
        License license = LICENSE_TOMBSTONE;
        if (streamInput.getVersion().before(Version.V_2_0_0)) {
            int size = streamInput.readVInt();
            List<License> licenses = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                licenses.add(License.readLicense(streamInput));
            }
            int numTrialLicenses = streamInput.readVInt();
            for (int i = 0; i < numTrialLicenses; i++) {
                byte[] data = decrypt(Base64.decode(streamInput.readString()));
                try (XContentParser trialLicenseParser = XContentFactory.xContent(XContentType.JSON).createParser(data)) {
                    trialLicenseParser.nextToken();
                    License pre20TrialLicense = License.fromXContent(trialLicenseParser);
                    licenses.add(TrialLicense.create(License.builder().fromPre20LicenseSpec(pre20TrialLicense)));
                }
            }
            // when we see read licenses from old pre v2.0,
            // we try to choose the license that has the latest issue date that is not expired
            CollectionUtil.timSort(licenses, License.LATEST_ISSUE_DATE_FIRST);
            long now = System.currentTimeMillis();
            for (License oldLicense : licenses) {
                if (oldLicense.expiryDate() > now) {
                    license = oldLicense;
                    break;
                }
            }
            // take the best expired license
            if (license == LICENSE_TOMBSTONE && !licenses.isEmpty()) {
                license = licenses.get(0);
            }
        } else {
            if (streamInput.readBoolean()) {
                license = License.readLicense(streamInput);
            }
        }
        return new LicensesMetaData(license);
    }

    private final static class Fields {
        private static final String SIGNED_LICENCES = "signed_licenses";
        private static final String TRIAL_LICENSES = "trial_licenses";
        private static final String LICENSE = "license";
    }
}