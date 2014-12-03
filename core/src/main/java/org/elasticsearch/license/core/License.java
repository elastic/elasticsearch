/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Data structure for license. Use {@link Builder} to build a license.
 * Provides serialization/deserialization & validation methods for license object
 */
public class License implements ToXContent {
    public final static int VERSION_START = 1;
    public final static int VERSION_CURRENT = VERSION_START;

    private final String uid;
    private final String issuer;
    private final String issuedTo;
    private final long issueDate;
    private final String type;
    private final String subscriptionType;
    private final String feature;
    private final String signature;
    private final long expiryDate;
    private final int maxNodes;

    private License(String uid, String issuer, String issuedTo, long issueDate, String type,
                    String subscriptionType, String feature, String signature, long expiryDate, int maxNodes) {
        this.uid = uid;
        this.issuer = issuer;
        this.issuedTo = issuedTo;
        this.issueDate = issueDate;
        this.type = type;
        this.subscriptionType = subscriptionType;
        this.feature = feature;
        this.signature = signature;
        this.expiryDate = expiryDate;
        this.maxNodes = maxNodes;
    }


    /**
     * @return a unique identifier for a license
     */
    public String uid() {
        return uid;
    }

    /**
     * @return type of the license [trial, subscription, internal]
     */
    public String type() {
        return type;
    }

    /**
     * @return subscription type of the license [none, silver, gold, platinum]
     */
    public String subscriptionType() {
        return subscriptionType;
    }

    /**
     * @return the issueDate in milliseconds
     */
    public long issueDate() {
        return issueDate;
    }

    /**
     * @return the featureType for the license [shield, marvel]
     */
    public String feature() {
        return feature;
    }

    /**
     * @return the expiry date in milliseconds
     */
    public long expiryDate() {
        return expiryDate;
    }

    /**
     * @return the maximum number of nodes this license has been issued for
     */
    public int maxNodes() {
        return maxNodes;
    }

    /**
     * @return a string representing the entity this licenses has been issued to
     */
    public String issuedTo() {
        return issuedTo;
    }

    /**
     * @return a string representing the entity responsible for issuing this license (internal)
     */
    public String issuer() {
        return issuer;
    }

    /**
     * @return a string representing the signature of the license used for license verification
     */
    public String signature() {
        return signature;
    }

    public void validate() {
        if (issuer == null) {
            throw new IllegalStateException("issuer can not be null");
        } else if (issuedTo == null) {
            throw new IllegalStateException("issuedTo can not be null");
        } else if (issueDate == -1) {
            throw new IllegalStateException("issueDate has to be set");
        } else if (type == null) {
            throw new IllegalStateException("type can not be null");
        } else if (subscriptionType == null) {
            throw new IllegalStateException("subscriptionType can not be null");
        } else if (uid == null) {
            throw new IllegalStateException("uid can not be null");
        } else if (feature == null) {
            throw new IllegalStateException("at least one feature has to be enabled");
        } else if (signature == null) {
            throw new IllegalStateException("signature can not be null");
        } else if (maxNodes == -1) {
            throw new IllegalStateException("maxNodes has to be set");
        } else if (expiryDate == -1) {
            throw new IllegalStateException("expiryDate has to be set");
        }
    }

    static License readLicense(StreamInput in) throws IOException {
        int version = in.readVInt(); // Version for future extensibility
        if (version > VERSION_CURRENT) {
            throw new ElasticsearchException("Unknown license version found, please upgrade all nodes to the latest elasticsearch-license plugin");
        }
        Builder builder = builder();
        builder.uid(in.readString());
        builder.type(in.readString());
        builder.subscriptionType(in.readString());
        builder.issueDate(in.readLong());
        builder.feature(in.readString());
        builder.expiryDate(in.readLong());
        builder.maxNodes(in.readInt());
        builder.issuedTo(in.readString());
        builder.issuer(in.readString());
        builder.signature(in.readOptionalString());
        return builder.build();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(VERSION_CURRENT);
        out.writeString(uid);
        out.writeString(type);
        out.writeString(subscriptionType);
        out.writeLong(issueDate);
        out.writeString(feature);
        out.writeLong(expiryDate);
        out.writeInt(maxNodes);
        out.writeString(issuedTo);
        out.writeString(issuer);
        out.writeOptionalString(signature);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean licenseSpecMode = params.paramAsBoolean(Licenses.LICENSE_SPEC_VIEW_MODE, false);
        boolean restViewMode = params.paramAsBoolean(Licenses.REST_VIEW_MODE, false);
        boolean previouslyHumanReadable = builder.humanReadable();
        if (licenseSpecMode && restViewMode) {
            throw new IllegalArgumentException("can have either " + Licenses.REST_VIEW_MODE + " or " + Licenses.LICENSE_SPEC_VIEW_MODE);
        } else if (restViewMode) {
            if (!previouslyHumanReadable) {
                builder.humanReadable(true);
            }
        }
        builder.startObject();
        if (restViewMode) {
            builder.field(XFields.STATUS, ((expiryDate - System.currentTimeMillis()) > 0l) ? "active" : "expired");
        }
        builder.field(XFields.UID, uid);
        builder.field(XFields.TYPE, type);
        builder.field(XFields.SUBSCRIPTION_TYPE, subscriptionType);
        builder.dateValueField(XFields.ISSUE_DATE_IN_MILLIS, XFields.ISSUE_DATE, issueDate);
        builder.field(XFields.FEATURE, feature);
        builder.dateValueField(XFields.EXPIRY_DATE_IN_MILLIS, XFields.EXPIRY_DATE, expiryDate);
        builder.field(XFields.MAX_NODES, maxNodes);
        builder.field(XFields.ISSUED_TO, issuedTo);
        builder.field(XFields.ISSUER, issuer);
        if (!licenseSpecMode && !restViewMode && signature != null) {
            builder.field(XFields.SIGNATURE, signature);
        }
        builder.endObject();
        if (restViewMode) {
            builder.humanReadable(previouslyHumanReadable);
        }
        return builder;
    }


    final static class Fields {
        static final String STATUS = "status";
        static final String UID = "uid";
        static final String TYPE = "type";
        static final String SUBSCRIPTION_TYPE = "subscription_type";
        static final String ISSUE_DATE_IN_MILLIS = "issue_date_in_millis";
        static final String ISSUE_DATE = "issue_date";
        static final String FEATURE = "feature";
        static final String EXPIRY_DATE_IN_MILLIS = "expiry_date_in_millis";
        static final String EXPIRY_DATE = "expiry_date";
        static final String MAX_NODES = "max_nodes";
        static final String ISSUED_TO = "issued_to";
        static final String ISSUER = "issuer";
        static final String SIGNATURE = "signature";
    }

    private final static class XFields {
        static final XContentBuilderString STATUS = new XContentBuilderString(Fields.STATUS);
        static final XContentBuilderString UID = new XContentBuilderString(Fields.UID);
        static final XContentBuilderString TYPE = new XContentBuilderString(Fields.TYPE);
        static final XContentBuilderString SUBSCRIPTION_TYPE = new XContentBuilderString(Fields.SUBSCRIPTION_TYPE);
        static final XContentBuilderString ISSUE_DATE_IN_MILLIS = new XContentBuilderString(Fields.ISSUE_DATE_IN_MILLIS);
        static final XContentBuilderString ISSUE_DATE = new XContentBuilderString(Fields.ISSUE_DATE);
        static final XContentBuilderString FEATURE = new XContentBuilderString(Fields.FEATURE);
        static final XContentBuilderString EXPIRY_DATE_IN_MILLIS = new XContentBuilderString(Fields.EXPIRY_DATE_IN_MILLIS);
        static final XContentBuilderString EXPIRY_DATE = new XContentBuilderString(Fields.EXPIRY_DATE);
        static final XContentBuilderString MAX_NODES = new XContentBuilderString(Fields.MAX_NODES);
        static final XContentBuilderString ISSUED_TO = new XContentBuilderString(Fields.ISSUED_TO);
        static final XContentBuilderString ISSUER = new XContentBuilderString(Fields.ISSUER);
        static final XContentBuilderString SIGNATURE = new XContentBuilderString(Fields.SIGNATURE);
    }

    private static long parseDate(XContentParser parser, String description, boolean endOfTheDay) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            return parser.longValue();
        } else {
            try {
                if (endOfTheDay) {
                    return DateUtils.endOfTheDay(parser.text());
                } else {
                    return DateUtils.beginningOfTheDay(parser.text());
                }
            } catch (IllegalArgumentException ex) {
                throw new ElasticsearchParseException("invalid " + description + " date format " + parser.text());
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String uid;
        private String issuer;
        private String issuedTo;
        private long issueDate = -1;
        private String type;
        private String subscriptionType = "none";
        private String feature;
        private String signature;
        private long expiryDate = -1;
        private int maxNodes = -1;


        public Builder uid(String uid) {
            this.uid = uid;
            return this;
        }

        public Builder issuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        public Builder issuedTo(String issuedTo) {
            this.issuedTo = issuedTo;
            return this;
        }

        public Builder issueDate(long issueDate) {
            this.issueDate = issueDate;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder subscriptionType(String subscriptionType) {
            this.subscriptionType = subscriptionType;
            return this;
        }

        public Builder feature(String feature) {
            this.feature = feature;
            return this;
        }

        public Builder expiryDate(long expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public Builder maxNodes(int maxNodes) {
            this.maxNodes = maxNodes;
            return this;
        }

        public Builder signature(String signature) {
            if (signature != null) {
                this.signature = signature;
            }
            return this;
        }

        public Builder fromLicenseSpec(License license, String signature) {
            return uid(license.uid())
                    .issuedTo(license.issuedTo())
                    .issueDate(license.issueDate())
                    .type(license.type())
                    .subscriptionType(license.subscriptionType())
                    .feature(license.feature())
                    .maxNodes(license.maxNodes())
                    .expiryDate(license.expiryDate())
                    .issuer(license.issuer())
                    .signature(signature);
        }

        public Builder fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token.isValue()) {
                            if (Fields.UID.equals(currentFieldName)) {
                                uid(parser.text());
                            } else if (Fields.TYPE.equals(currentFieldName)) {
                                type(parser.text());
                            } else if (Fields.SUBSCRIPTION_TYPE.equals(currentFieldName)) {
                                subscriptionType(parser.text());
                            } else if (Fields.ISSUE_DATE.equals(currentFieldName)) {
                                issueDate(parseDate(parser, "issue", false));
                            } else if (Fields.ISSUE_DATE_IN_MILLIS.equals(currentFieldName)) {
                                issueDate(parser.longValue());
                            } else if (Fields.FEATURE.equals(currentFieldName)) {
                                feature(parser.text());
                            } else if (Fields.EXPIRY_DATE.equals(currentFieldName)) {
                                expiryDate(parseDate(parser, "expiration", true));
                            } else if (Fields.EXPIRY_DATE_IN_MILLIS.equals(currentFieldName)) {
                                expiryDate(parser.longValue());
                            } else if (Fields.MAX_NODES.equals(currentFieldName)) {
                                maxNodes(parser.intValue());
                            } else if (Fields.ISSUED_TO.equals(currentFieldName)) {
                                issuedTo(parser.text());
                            } else if (Fields.ISSUER.equals(currentFieldName)) {
                                issuer(parser.text());
                            } else if (Fields.SIGNATURE.equals(currentFieldName)) {
                                signature(parser.text());
                            }
                            // Ignore unknown elements - might be new version of license
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            // It was probably created by newer version - ignoring
                            parser.skipChildren();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            // It was probably created by newer version - ignoring
                            parser.skipChildren();
                        }
                    }
                }
            } else {
                throw new ElasticsearchParseException("failed to parse licenses expected a license object");
            }
            return this;
        }

        public License build() {
            return new License(uid, issuer, issuedTo, issueDate, type,
                    subscriptionType, feature, signature, expiryDate, maxNodes);
        }

        public Builder validate() {
            if (issuer == null) {
                throw new IllegalStateException("issuer can not be null");
            } else if (issuedTo == null) {
                throw new IllegalStateException("issuedTo can not be null");
            } else if (issueDate == -1) {
                throw new IllegalStateException("issueDate has to be set");
            } else if (type == null) {
                throw new IllegalStateException("type can not be null");
            } else if (subscriptionType == null) {
                throw new IllegalStateException("subscriptionType can not be null");
            } else if (uid == null) {
                throw new IllegalStateException("uid can not be null");
            } else if (feature == null) {
                throw new IllegalStateException("at least one feature has to be enabled");
            } else if (signature == null) {
                throw new IllegalStateException("signature can not be null");
            } else if (maxNodes == -1) {
                throw new IllegalStateException("maxNodes has to be set");
            } else if (expiryDate == -1) {
                throw new IllegalStateException("expiryDate has to be set");
            }
            return this;
        }
    }

}
