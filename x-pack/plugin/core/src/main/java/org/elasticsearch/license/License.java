/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.RestApiVersion.onOrAfter;

/**
 * Data structure for license. Use {@link Builder} to build a license.
 * Provides serialization/deserialization &amp; validation methods for license object
 */
public class License implements ToXContentObject {

    public enum LicenseType {
        BASIC,
        STANDARD,
        GOLD,
        PLATINUM,
        ENTERPRISE,
        TRIAL;

        public String getTypeName() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static LicenseType parse(String type) throws IllegalArgumentException {
            try {
                return LicenseType.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unrecognised license type [ " + type + "], supported license types are ["
                    + Stream.of(values()).map(LicenseType::getTypeName).collect(Collectors.joining(",")) + "]");
            }
        }

        /**
         * Backward compatible license type parsing for older license models
         */
        public static LicenseType resolve(License license) {
            if (license.version == VERSION_START) {
                // in 1.x: the acceptable values for 'subscription_type': none | dev | silver | gold | platinum
                return resolve(license.subscriptionType);
            } else {
                // in 2.x: the acceptable values for 'type': trial | basic | silver | dev | gold | platinum
                // in 5.x: the acceptable values for 'type': trial | basic | standard | dev | gold | platinum
                // in 6.x: the acceptable values for 'type': trial | basic | standard | dev | gold | platinum
                // in 7.x: the acceptable values for 'type': trial | basic | standard | dev | gold | platinum | enterprise
                return resolve(license.type);
            }
        }

        /**
         * Backward compatible license type parsing for older license models
         */
        static LicenseType resolve(String name) {
            switch (name.toLowerCase(Locale.ROOT)) {
                case "missing":
                    return null;
                case "trial":
                case "none": // bwc for 1.x subscription_type field
                case "dev": // bwc for 1.x subscription_type field
                case "development": // bwc for 1.x subscription_type field
                    return TRIAL;
                case "basic":
                    return BASIC;
                case "standard":
                    return STANDARD;
                case "silver":
                case "gold":
                    return GOLD;
                case "platinum":
                case "cloud_internal":
                case "internal": // bwc for 1.x subscription_type field
                    return PLATINUM;
                case "enterprise":
                    return ENTERPRISE;
                default:
                    throw new IllegalArgumentException("unknown license type [" + name + "]");
            }
        }

        static boolean isBasic(String typeName) {
            return BASIC.getTypeName().equals(typeName);
        }

        static boolean isTrial(String typeName) {
            return TRIAL.getTypeName().equals(typeName);
        }

        static boolean isEnterprise(String typeName) {
            return ENTERPRISE.getTypeName().equals(typeName);
        }

    }

    public static final int VERSION_START = 1;
    public static final int VERSION_NO_FEATURE_TYPE = 2;
    public static final int VERSION_START_DATE = 3;
    public static final int VERSION_CRYPTO_ALGORITHMS = 4;
    public static final int VERSION_ENTERPRISE = 5;
    public static final int VERSION_CURRENT = VERSION_ENTERPRISE;

    /**
     * XContent param name to deserialize license(s) with
     * an additional <code>status</code> field, indicating whether a
     * particular license is 'active' or 'expired' and no signature
     * and in a human readable format
     */
    public static final String REST_VIEW_MODE = "rest_view";
    /**
     * XContent param name to deserialize license(s) with
     * no signature
     */
    public static final String LICENSE_SPEC_VIEW_MODE = "license_spec_view";
    /**
     * XContent param name to deserialize licenses according
     * to a specific license version
     */
    public static final String LICENSE_VERSION_MODE = "license_version";
    /**
     * Set on v7 rest compatible requests only
     * XContent param name to map the "enterprise" license type to "platinum"
     * for backwards compatibility with older clients
     */
    public static final String XCONTENT_HIDE_ENTERPRISE = "hide_enterprise";

    public static final Comparator<License> LATEST_ISSUE_DATE_FIRST = Comparator.comparing(License::issueDate).reversed();

    private final int version;
    private final String uid;
    private final String issuer;
    private final String issuedTo;
    private final long issueDate;
    private final String type;
    private final String subscriptionType;
    private final String feature;
    private final String signature;
    private final long expiryDate;
    private final long startDate;
    private final int maxNodes;
    private final int maxResourceUnits;
    private final OperationMode operationMode;

    /**
     * Decouples operation mode of a license from the license type value.
     * <p>
     * Note: The mode indicates features that should be made available, but it does not indicate whether the license is active!
     * <p>
     * The id byte is used for ordering operation modes
     */
    public enum OperationMode {
        MISSING((byte) 0),
        TRIAL((byte) 1),
        BASIC((byte) 2),
        STANDARD((byte) 3),
        GOLD((byte) 4),
        PLATINUM((byte) 5),
        ENTERPRISE((byte) 6);

        private final byte id;

        OperationMode(byte id) {
            this.id = id;
        }

        /**
         * Returns non-zero positive number when <code>opMode1</code> is greater than <code>opMode2</code>
         */
        public static int compare(OperationMode opMode1, OperationMode opMode2) {
            return Integer.compare(opMode1.id, opMode2.id);
        }

        /**
         * Determine the operating mode for a license type
         *
         * @see LicenseType#resolve(License)
         * @see #parse(String)
         */
        public static OperationMode resolve(LicenseType type) {
            if (type == null) {
                return MISSING;
            }
            switch (type) {
                case BASIC:
                    return BASIC;
                case STANDARD:
                    return STANDARD;
                case GOLD:
                    return GOLD;
                case PLATINUM:
                    return PLATINUM;
                case ENTERPRISE:
                    return ENTERPRISE;
                case TRIAL:
                    return TRIAL;
                default:
                    throw new IllegalArgumentException("unsupported license type [" + type.getTypeName() + "]");
            }
        }

        /**
         * Parses an {@code OperatingMode} from a String.
         * The string must name an operating mode, and not a licensing level (that is, it cannot parse old style license levels
         * such as "dev" or "silver").
         *
         * @see #description()
         */
        public static OperationMode parse(String mode) {
            try {
                return OperationMode.valueOf(mode.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unrecognised license operating mode [ " + mode + "], supported modes are ["
                    + Stream.of(values()).map(OperationMode::description).collect(Collectors.joining(",")) + "]");
            }
        }

        public String description() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private License(int version, String uid, String issuer, String issuedTo, long issueDate, String type, String subscriptionType,
                    String feature, String signature, long expiryDate, int maxNodes, int maxResourceUnits, long startDate) {
        this.version = version;
        this.uid = uid;
        this.issuer = issuer;
        this.issuedTo = issuedTo;
        this.issueDate = issueDate;
        this.type = type;
        this.subscriptionType = subscriptionType;
        this.feature = feature;
        this.signature = signature;
        // We will validate that only a basic license can have the BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS
        // in the validate() method.
        if (expiryDate == -1) {
            this.expiryDate = LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
        } else {
            this.expiryDate = expiryDate;
        }
        this.maxNodes = maxNodes;
        this.maxResourceUnits = maxResourceUnits;
        this.startDate = startDate;
        this.operationMode = OperationMode.resolve(LicenseType.resolve(this));
        validate();
    }

    /**
     * @return version of the license
     */
    public int version() {
        return version;
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
     * @return the issueDate in milliseconds
     */
    public long issueDate() {
        return issueDate;
    }

    /**
     * @return the startDate in milliseconds
     */
    public long startDate() {
        return startDate;
    }

    /**
     * @return the expiry date in milliseconds
     */
    public long expiryDate() {
        return expiryDate;
    }

    /**
     * @return the maximum number of nodes this license has been issued for, or {@code -1} if this license is not node based.
     */
    public int maxNodes() {
        return maxNodes;
    }

    /**
     * @return the maximum number of "resource units" this license has been issued for, or {@code -1} if this license is not resource based.
     * A "resource unit" is a measure of computing power (RAM/CPU), the definition of which is maintained outside of the license format,
     * or this class.
     */
    public int maxResourceUnits() {
        return maxResourceUnits;
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

    /**
     * @return the operation mode of the license as computed from the license type or from
     * the license mode file
     */
    public OperationMode operationMode() {
        synchronized (this) {
            if (canReadOperationModeFromFile() && operationModeFileWatcher != null) {
                return operationModeFileWatcher.getCurrentOperationMode();
            }
        }
        return operationMode;
    }

    private boolean canReadOperationModeFromFile() {
        return type.equals("cloud_internal");
    }

    private volatile OperationModeFileWatcher operationModeFileWatcher;

    /**
     * Sets the operation mode file watcher for the license and initializes the
     * file watcher when the license type allows to override operation mode from file
     */
    public synchronized void setOperationModeFileWatcher(final OperationModeFileWatcher operationModeFileWatcher) {
        this.operationModeFileWatcher = operationModeFileWatcher;
        if (canReadOperationModeFromFile()) {
            this.operationModeFileWatcher.init();
        }
    }

    /**
     * Removes operation mode file watcher, so unused license objects can be gc'ed
     */
    public synchronized void removeOperationModeFileWatcher() {
        this.operationModeFileWatcher = null;
    }

    /**
     * @return the current license's status
     */
    public LicenseStatus status() {
        long now = System.currentTimeMillis();
        if (issueDate > now) {
            return LicenseStatus.INVALID;
        } else if (expiryDate < now) {
            return LicenseStatus.EXPIRED;
        }
        return LicenseStatus.ACTIVE;
    }

    private void validate() {
        if (issuer == null) {
            throw new IllegalStateException("issuer can not be null");
        } else if (issuedTo == null) {
            throw new IllegalStateException("issuedTo can not be null");
        } else if (issueDate == -1) {
            throw new IllegalStateException("issueDate has to be set");
        } else if (type == null) {
            throw new IllegalStateException("type can not be null");
        } else if (subscriptionType == null && version == VERSION_START) {
            throw new IllegalStateException("subscriptionType can not be null");
        } else if (uid == null) {
            throw new IllegalStateException("uid can not be null");
        } else if (feature == null && version == VERSION_START) {
            throw new IllegalStateException("feature can not be null");
        } else if (expiryDate == -1) {
            throw new IllegalStateException("expiryDate has to be set");
        } else if (expiryDate == LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS && LicenseType.isBasic(type) == false) {
            throw new IllegalStateException("only basic licenses are allowed to have no expiration");
        }

        if (LicenseType.isEnterprise(type) && version < VERSION_ENTERPRISE) {
            throw new IllegalStateException("license type [" + type + "] is not a valid for version [" + version + "] licenses");
        }
        validateLimits(type, maxNodes, maxResourceUnits);
    }

    private static void validateLimits(String type, int maxNodes, int maxResourceUnits) {
        if (LicenseType.isEnterprise(type)) {
            if (maxResourceUnits == -1) {
                throw new IllegalStateException("maxResourceUnits must be set for enterprise licenses (type=[" + type + "])");
            } else if (maxNodes != -1) {
                throw new IllegalStateException("maxNodes may not be set for enterprise licenses (type=[" + type + "])");
            }
        } else {
            if (maxNodes == -1) {
                throw new IllegalStateException("maxNodes has to be set");
            } else if (maxResourceUnits != -1) {
                throw new IllegalStateException("maxResourceUnits may only be set for enterprise licenses (not permitted for type=[" +
                    type + "])");
            }
        }
    }

    public static License readLicense(StreamInput in) throws IOException {
        int version = in.readVInt(); // Version for future extensibility
        if (version > VERSION_CURRENT) {
            throw new ElasticsearchException("Unknown license version found, please upgrade all nodes to the latest elasticsearch release");
        }
        Builder builder = builder();
        builder.version(version);
        builder.uid(in.readString());
        builder.type(in.readString());
        if (version == VERSION_START) {
            builder.subscriptionType(in.readString());
        }
        builder.issueDate(in.readLong());
        if (version == VERSION_START) {
            builder.feature(in.readString());
        }
        builder.expiryDate(in.readLong());
        builder.maxNodes(in.readInt());
        if (version >= VERSION_ENTERPRISE) {
            builder.maxResourceUnits(in.readInt());
        }
        builder.issuedTo(in.readString());
        builder.issuer(in.readString());
        builder.signature(in.readOptionalString());
        if (version >= VERSION_START_DATE) {
            builder.startDate(in.readLong());
        }
        return builder.build();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(version);
        out.writeString(uid);
        out.writeString(type);
        if (version == VERSION_START) {
            out.writeString(subscriptionType);
        }
        out.writeLong(issueDate);
        if (version == VERSION_START) {
            out.writeString(feature);
        }
        out.writeLong(expiryDate);
        out.writeInt(maxNodes);
        if (version >= VERSION_ENTERPRISE) {
            out.writeInt(maxResourceUnits);
        }
        out.writeString(issuedTo);
        out.writeString(issuer);
        out.writeOptionalString(signature);
        if (version >= VERSION_START_DATE) {
            out.writeLong(startDate);
        }
    }

    @Override
    public String toString() {
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (IOException e) {
            return "";
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toInnerXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        boolean licenseSpecMode = params.paramAsBoolean(LICENSE_SPEC_VIEW_MODE, false);
        boolean restViewMode = params.paramAsBoolean(REST_VIEW_MODE, false);
        boolean hideEnterprise = params.paramAsBoolean(XCONTENT_HIDE_ENTERPRISE, false);

        boolean previouslyHumanReadable = builder.humanReadable();
        if (licenseSpecMode && restViewMode) {
            throw new IllegalArgumentException("can have either " + REST_VIEW_MODE + " or " + LICENSE_SPEC_VIEW_MODE);
        } else if (restViewMode) {
            if (previouslyHumanReadable == false) {
                builder.humanReadable(true);
            }
        }
        final int version;
        if (params.param(LICENSE_VERSION_MODE) != null && restViewMode) {
            version = Integer.parseInt(params.param(LICENSE_VERSION_MODE));
        } else {
            version = this.version;
        }
        if (restViewMode) {
            builder.field(Fields.STATUS, status().label());
        }
        builder.field(Fields.UID, uid);
        final String bwcType = hideEnterprise && LicenseType.isEnterprise(type) ? LicenseType.PLATINUM.getTypeName() : type;
        builder.field(Fields.TYPE, bwcType);
        if (version == VERSION_START) {
            builder.field(Fields.SUBSCRIPTION_TYPE, subscriptionType);
        }
        builder.timeField(Fields.ISSUE_DATE_IN_MILLIS, Fields.ISSUE_DATE, issueDate);
        if (version == VERSION_START) {
            builder.field(Fields.FEATURE, feature);
        }

        if (expiryDate != LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS) {
            builder.timeField(Fields.EXPIRY_DATE_IN_MILLIS, Fields.EXPIRY_DATE, expiryDate);
        }
        if (hideEnterprise && builder.getRestApiVersion().matches(onOrAfter(RestApiVersion.V_7))) {
            builder.field(Fields.MAX_NODES,maxNodes == -1  ? maxResourceUnits : maxNodes);
        }else if (version >= VERSION_ENTERPRISE ) {
            builder.field(Fields.MAX_NODES, maxNodes == -1 ? null : maxNodes);
            builder.field(Fields.MAX_RESOURCE_UNITS, maxResourceUnits == -1 ? null : maxResourceUnits);
        } else {
            builder.field(Fields.MAX_NODES, maxNodes);
        }

        builder.field(Fields.ISSUED_TO, issuedTo);
        builder.field(Fields.ISSUER, issuer);
        if (licenseSpecMode == false && restViewMode == false && signature != null) {
            builder.field(Fields.SIGNATURE, signature);
        }
        if (restViewMode) {
            builder.humanReadable(previouslyHumanReadable);
        }
        if (version >= VERSION_START_DATE) {
            builder.timeField(Fields.START_DATE_IN_MILLIS, Fields.START_DATE, startDate);
        }
        return builder;
    }

    public static License fromXContent(XContentParser parser) throws IOException {
        Builder builder = new Builder();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                token = parser.nextToken();
                if (token.isValue()) {
                    if (Fields.UID.equals(currentFieldName)) {
                        builder.uid(parser.text());
                    } else if (Fields.TYPE.equals(currentFieldName)) {
                        builder.type(parser.text());
                    } else if (Fields.SUBSCRIPTION_TYPE.equals(currentFieldName)) {
                        builder.subscriptionType(parser.text());
                    } else if (Fields.ISSUE_DATE.equals(currentFieldName)) {
                        builder.issueDate(parseDate(parser, "issue", false));
                    } else if (Fields.ISSUE_DATE_IN_MILLIS.equals(currentFieldName)) {
                        builder.issueDate(parser.longValue());
                    } else if (Fields.FEATURE.equals(currentFieldName)) {
                        builder.feature(parser.text());
                    } else if (Fields.EXPIRY_DATE.equals(currentFieldName)) {
                        builder.expiryDate(parseDate(parser, "expiration", true));
                    } else if (Fields.EXPIRY_DATE_IN_MILLIS.equals(currentFieldName)) {
                        builder.expiryDate(parser.longValue());
                    } else if (Fields.START_DATE.equals(currentFieldName)) {
                        builder.startDate(parseDate(parser, "start", false));
                    } else if (Fields.START_DATE_IN_MILLIS.equals(currentFieldName)) {
                        builder.startDate(parser.longValue());
                    } else if (Fields.MAX_NODES.equals(currentFieldName)) {
                        builder.maxNodes(parser.intValue());
                    } else if (Fields.MAX_RESOURCE_UNITS.equals(currentFieldName)) {
                        builder.maxResourceUnits(parser.intValue());
                    } else if (Fields.ISSUED_TO.equals(currentFieldName)) {
                        builder.issuedTo(parser.text());
                    } else if (Fields.ISSUER.equals(currentFieldName)) {
                        builder.issuer(parser.text());
                    } else if (Fields.SIGNATURE.equals(currentFieldName)) {
                        builder.signature(parser.text());
                    } else if (Fields.VERSION.equals(currentFieldName)) {
                        builder.version(parser.intValue());
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
        // not a license spec
        if (builder.signature != null) {
            int version;
            // In case the signature is truncated/malformed we might end up with fewer than 4 bytes in the byteBuffer
            // or with a string that cannot be base64 decoded. In either case return a more friendly error instead of
            // just throwing the BufferUnderflowException or the IllegalArgumentException
            try {
                byte[] signatureBytes = Base64.getDecoder().decode(builder.signature);
                ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
                version = byteBuffer.getInt();
            } catch (Exception e) {
                throw new ElasticsearchException("malformed signature for license [" + builder.uid + "]", e);
            }
            // we take the absolute version, because negative versions
            // mean that the license was generated by the cluster (see TrialLicense)
            // and positive version means that the license was signed
            if (version < 0) {
                version *= -1;
            }
            if (version == 0) {
                throw new ElasticsearchException("malformed signature for license [" + builder.uid + "]");
            } else if (version > VERSION_CURRENT) {
                throw new ElasticsearchException("Unknown license version found, please upgrade all nodes to the latest " +
                    "elasticsearch-license plugin");
            }
            // signature version is the source of truth
            builder.version(version);
        }
        return builder.build();
    }

    /**
     * Returns true if the license was auto-generated (by license plugin),
     * false otherwise
     */
    public static boolean isAutoGeneratedLicense(String signature) {
        try {
            byte[] signatureBytes = Base64.getDecoder().decode(signature);
            ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
            return byteBuffer.getInt() < 0;
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(e);
        }
    }

    public static License fromSource(BytesReference bytes, XContentType xContentType) throws IOException {
        if (bytes == null || bytes.length() == 0) {
            throw new ElasticsearchParseException("failed to parse license - no content provided");
        }
        if (xContentType == null) {
            throw new ElasticsearchParseException("failed to parse license - no content-type provided");
        }
        // EMPTY is safe here because we don't call namedObject
        try (InputStream byteStream = bytes.streamInput();
             XContentParser parser = xContentType.xContent()
                 .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, byteStream)) {
            License license = null;
            if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    if (Fields.LICENSES.equals(currentFieldName)) {
                        final List<License> pre20Licenses = new ArrayList<>();
                        if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                pre20Licenses.add(License.fromXContent(parser));
                            }
                            // take the latest issued unexpired license
                            CollectionUtil.timSort(pre20Licenses, LATEST_ISSUE_DATE_FIRST);
                            long now = System.currentTimeMillis();
                            for (License oldLicense : pre20Licenses) {
                                if (oldLicense.expiryDate() > now) {
                                    license = oldLicense;
                                    break;
                                }
                            }
                            if (license == null && pre20Licenses.isEmpty() == false) {
                                license = pre20Licenses.get(0);
                            }
                        } else {
                            throw new ElasticsearchParseException("failed to parse licenses expected an array of licenses");
                        }
                    } else if (Fields.LICENSE.equals(currentFieldName)) {
                        license = License.fromXContent(parser);
                    }
                    // Ignore all other fields - might be created with new version
                } else {
                    throw new ElasticsearchParseException("failed to parse licenses expected field");
                }
            } else {
                throw new ElasticsearchParseException("failed to parse licenses expected start object");
            }
            return license;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        License license = (License) o;

        if (issueDate != license.issueDate) return false;
        if (expiryDate != license.expiryDate) return false;
        if (startDate != license.startDate) return false;
        if (maxNodes != license.maxNodes) return false;
        if (version != license.version) return false;
        return Objects.equals(uid, license.uid)
            && Objects.equals(issuer, license.issuer)
            && Objects.equals(issuedTo, license.issuedTo)
            && Objects.equals(type, license.type)
            && Objects.equals(subscriptionType, license.subscriptionType)
            && Objects.equals(feature, license.feature)
            && Objects.equals(signature, license.signature);
    }

    @Override
    public int hashCode() {
        int result = uid != null ? uid.hashCode() : 0;
        result = 31 * result + (issuer != null ? issuer.hashCode() : 0);
        result = 31 * result + (issuedTo != null ? issuedTo.hashCode() : 0);
        result = 31 * result + (int) (issueDate ^ (issueDate >>> 32));
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (subscriptionType != null ? subscriptionType.hashCode() : 0);
        result = 31 * result + (feature != null ? feature.hashCode() : 0);
        result = 31 * result + (signature != null ? signature.hashCode() : 0);
        result = 31 * result + (int) (expiryDate ^ (expiryDate >>> 32));
        result = 31 * result + (int) (startDate ^ (startDate >>> 32));
        result = 31 * result + maxNodes;
        result = 31 * result + version;
        return result;
    }

    public static final class Fields {
        public static final String STATUS = "status";
        public static final String UID = "uid";
        public static final String TYPE = "type";
        public static final String SUBSCRIPTION_TYPE = "subscription_type";
        public static final String ISSUE_DATE_IN_MILLIS = "issue_date_in_millis";
        public static final String ISSUE_DATE = "issue_date";
        public static final String FEATURE = "feature";
        public static final String EXPIRY_DATE_IN_MILLIS = "expiry_date_in_millis";
        public static final String EXPIRY_DATE = "expiry_date";
        public static final String START_DATE_IN_MILLIS = "start_date_in_millis";
        public static final String START_DATE = "start_date";
        public static final String MAX_NODES = "max_nodes";
        public static final String MAX_RESOURCE_UNITS = "max_resource_units";
        public static final String ISSUED_TO = "issued_to";
        public static final String ISSUER = "issuer";
        public static final String VERSION = "version";
        public static final String SIGNATURE = "signature";

        public static final String LICENSES = "licenses";
        public static final String LICENSE = "license";

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
        private int version = License.VERSION_CURRENT;
        private String uid;
        private String issuer;
        private String issuedTo;
        private long issueDate = -1;
        private String type;
        private String subscriptionType;
        private String feature;
        private String signature;
        private long expiryDate = -1;
        private long startDate = -1;
        private int maxNodes = -1;
        private int maxResourceUnits = -1;

        public Builder uid(String uid) {
            this.uid = uid;
            return this;
        }

        public Builder version(int version) {
            this.version = version;
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

        public Builder type(LicenseType type) {
            return type(type.getTypeName());
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

        public Builder maxResourceUnits(int maxUnits) {
            this.maxResourceUnits = maxUnits;
            return this;
        }

        public Builder signature(String signature) {
            if (signature != null) {
                this.signature = signature;
            }
            return this;
        }

        public Builder startDate(long startDate) {
            this.startDate = startDate;
            return this;
        }

        public Builder fromLicenseSpec(License license, String signature) {
            return uid(license.uid())
                .version(license.version())
                .issuedTo(license.issuedTo())
                .issueDate(license.issueDate())
                .startDate(license.startDate())
                .type(license.type())
                .subscriptionType(license.subscriptionType)
                .feature(license.feature)
                .maxNodes(license.maxNodes())
                .maxResourceUnits(license.maxResourceUnits())
                .expiryDate(license.expiryDate())
                .issuer(license.issuer())
                .signature(signature);
        }

        /**
         * Returns a builder that converts pre 2.0 licenses
         * to the new license format
         */
        public Builder fromPre20LicenseSpec(License pre20License) {
            return uid(pre20License.uid())
                .issuedTo(pre20License.issuedTo())
                .issueDate(pre20License.issueDate())
                .maxNodes(pre20License.maxNodes())
                .expiryDate(pre20License.expiryDate());
        }

        public License build() {
            return new License(version, uid, issuer, issuedTo, issueDate, type,
                subscriptionType, feature, signature, expiryDate, maxNodes, maxResourceUnits, startDate);
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
            } else if (uid == null) {
                throw new IllegalStateException("uid can not be null");
            } else if (signature == null) {
                throw new IllegalStateException("signature can not be null");
            } else if (expiryDate == -1) {
                throw new IllegalStateException("expiryDate has to be set");
            }
            validateLimits(type, maxNodes, maxResourceUnits);
            return this;
        }

    }

}
