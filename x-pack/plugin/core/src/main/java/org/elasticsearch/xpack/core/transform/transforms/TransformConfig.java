/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator.SourceDestValidation;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the configuration details of a data frame transform
 */
public class TransformConfig extends AbstractDiffable<TransformConfig> implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_config";
    public static final ParseField HEADERS = new ParseField("headers");
    /** Version in which {@code FieldCapabilitiesRequest.runtime_fields} field was introduced. */
    private static final Version FIELD_CAPS_RUNTIME_MAPPINGS_INTRODUCED_VERSION = Version.V_7_12_0;

    /** Specifies all the possible transform functions. */
    public enum Function {
        PIVOT, LATEST;

        private final ParseField parseField;

        Function() {
            this.parseField = new ParseField(name().toLowerCase(Locale.ROOT));
        }

        public ParseField getParseField() {
            return parseField;
        }
    }
    private static final ConstructingObjectParser<TransformConfig, String> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TransformConfig, String> LENIENT_PARSER = createParser(true);
    static final int MAX_DESCRIPTION_LENGTH = 1_000;

    private final String id;
    private final SourceConfig source;
    private final DestConfig dest;
    private final TimeValue frequency;
    private final SyncConfig syncConfig;
    private final SettingsConfig settings;
    private final RetentionPolicyConfig retentionPolicyConfig;
    private final String description;
    // headers store the user context from the creating user, which allows us to run the transform as this user
    // the header only contains name, groups and other context but no authorization keys
    private Map<String, String> headers;
    private Version transformVersion;
    private Instant createTime;

    private final PivotConfig pivotConfig;
    private final LatestConfig latestConfig;

    private static void validateStrictParsingParams(Object arg, String parameterName) {
        if (arg != null) {
            throw new IllegalArgumentException("Found [" + parameterName + "], not allowed for strict parsing");
        }
    }

    private static ConstructingObjectParser<TransformConfig, String> createParser(boolean lenient) {
        ConstructingObjectParser<TransformConfig, String> parser = new ConstructingObjectParser<>(NAME, lenient, (args, optionalId) -> {
            String id = (String) args[0];

            // if the id has been specified in the body and the path, they must match
            if (id == null) {
                id = optionalId;
            } else if (optionalId != null && id.equals(optionalId) == false) {
                throw new IllegalArgumentException(
                    TransformMessages.getMessage(TransformMessages.REST_PUT_TRANSFORM_INCONSISTENT_ID, id, optionalId)
                );
            }

            SourceConfig source = (SourceConfig) args[1];
            DestConfig dest = (DestConfig) args[2];

            TimeValue frequency = args[3] == null
                ? null
                : TimeValue.parseTimeValue((String) args[3], TransformField.FREQUENCY.getPreferredName());

            SyncConfig syncConfig = (SyncConfig) args[4];
            // ignored, only for internal storage: String docType = (String) args[5];

            if (lenient == false) {
                // on strict parsing do not allow injection of headers, transform version, or create time
                validateStrictParsingParams(args[6], HEADERS.getPreferredName());
                validateStrictParsingParams(args[12], TransformField.CREATE_TIME.getPreferredName());
                validateStrictParsingParams(args[13], TransformField.VERSION.getPreferredName());
                // exactly one function must be defined
                if ((args[7] == null) == (args[8] == null)) {
                    throw new IllegalArgumentException(TransformMessages.TRANSFORM_CONFIGURATION_BAD_FUNCTION_COUNT);
                }
            }

            @SuppressWarnings("unchecked")
            Map<String, String> headers = (Map<String, String>) args[6];

            PivotConfig pivotConfig = (PivotConfig) args[7];
            LatestConfig latestConfig = (LatestConfig) args[8];
            String description = (String) args[9];
            SettingsConfig settings = (SettingsConfig) args[10];
            RetentionPolicyConfig retentionPolicyConfig = (RetentionPolicyConfig) args[11];

            return new TransformConfig(
                id,
                source,
                dest,
                frequency,
                syncConfig,
                headers,
                pivotConfig,
                latestConfig,
                description,
                settings,
                retentionPolicyConfig,
                (Instant) args[12],
                (String) args[13]
            );
        });

        parser.declareString(optionalConstructorArg(), TransformField.ID);
        parser.declareObject(constructorArg(), (p, c) -> SourceConfig.fromXContent(p, lenient), TransformField.SOURCE);
        parser.declareObject(constructorArg(), (p, c) -> DestConfig.fromXContent(p, lenient), TransformField.DESTINATION);
        parser.declareString(optionalConstructorArg(), TransformField.FREQUENCY);
        parser.declareNamedObject(optionalConstructorArg(), (p, c, n) -> p.namedObject(SyncConfig.class, n, c), TransformField.SYNC);
        parser.declareString(optionalConstructorArg(), TransformField.INDEX_DOC_TYPE);
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.mapStrings(), HEADERS);
        parser.declareObject(optionalConstructorArg(), (p, c) -> PivotConfig.fromXContent(p, lenient), Function.PIVOT.getParseField());
        parser.declareObject(optionalConstructorArg(), (p, c) -> LatestConfig.fromXContent(p, lenient), Function.LATEST.getParseField());
        parser.declareString(optionalConstructorArg(), TransformField.DESCRIPTION);
        parser.declareObject(optionalConstructorArg(), (p, c) -> SettingsConfig.fromXContent(p, lenient), TransformField.SETTINGS);
        parser.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(RetentionPolicyConfig.class, n, c),
            TransformField.RETENTION_POLICY
        );
        parser.declareField(
            optionalConstructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, TransformField.CREATE_TIME.getPreferredName()),
            TransformField.CREATE_TIME,
            ObjectParser.ValueType.VALUE
        );
        parser.declareString(optionalConstructorArg(), TransformField.VERSION);
        return parser;
    }

    public static String documentId(String transformId) {
        return NAME + "-" + transformId;
    }

    public TransformConfig(
        final String id,
        final SourceConfig source,
        final DestConfig dest,
        final TimeValue frequency,
        final SyncConfig syncConfig,
        final Map<String, String> headers,
        final PivotConfig pivotConfig,
        final LatestConfig latestConfig,
        final String description,
        final SettingsConfig settings,
        final RetentionPolicyConfig retentionPolicyConfig,
        final Instant createTime,
        final String version
    ) {
        this.id = ExceptionsHelper.requireNonNull(id, TransformField.ID.getPreferredName());
        this.source = ExceptionsHelper.requireNonNull(source, TransformField.SOURCE.getPreferredName());
        this.dest = ExceptionsHelper.requireNonNull(dest, TransformField.DESTINATION.getPreferredName());
        this.frequency = frequency;
        this.syncConfig = syncConfig;
        this.setHeaders(headers == null ? Collections.emptyMap() : headers);
        this.pivotConfig = pivotConfig;
        this.latestConfig = latestConfig;
        this.description = description;
        this.settings = settings == null ? new SettingsConfig() : settings;
        this.retentionPolicyConfig = retentionPolicyConfig;
        if (this.description != null && this.description.length() > MAX_DESCRIPTION_LENGTH) {
            throw new IllegalArgumentException("[description] must be less than 1000 characters in length.");
        }
        this.createTime = createTime == null ? null : Instant.ofEpochMilli(createTime.toEpochMilli());
        this.transformVersion = version == null ? null : Version.fromString(version);
    }

    public TransformConfig(final StreamInput in) throws IOException {
        id = in.readString();
        source = new SourceConfig(in);
        dest = new DestConfig(in);
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            frequency = in.readOptionalTimeValue();
        } else {
            frequency = null;
        }
        setHeaders(in.readMap(StreamInput::readString, StreamInput::readString));
        pivotConfig = in.readOptionalWriteable(PivotConfig::new);
        latestConfig = in.readOptionalWriteable(LatestConfig::new);
        description = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            syncConfig = in.readOptionalNamedWriteable(SyncConfig.class);
            createTime = in.readOptionalInstant();
            transformVersion = in.readBoolean() ? Version.readVersion(in) : null;
        } else {
            syncConfig = null;
            createTime = null;
            transformVersion = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
            settings = new SettingsConfig(in);
        } else {
            settings = new SettingsConfig();
        }
        if (in.getVersion().onOrAfter(Version.V_7_12_0)) {
            retentionPolicyConfig = in.readOptionalNamedWriteable(RetentionPolicyConfig.class);
        } else {
            retentionPolicyConfig = null;
        }
    }

    public String getId() {
        return id;
    }

    public SourceConfig getSource() {
        return source;
    }

    public DestConfig getDestination() {
        return dest;
    }

    public TimeValue getFrequency() {
        return frequency;
    }

    public SyncConfig getSyncConfig() {
        return syncConfig;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public TransformConfig setHeaders(Map<String, String> headers) {
        this.headers = headers;
        return this;
    }

    public Version getVersion() {
        return transformVersion;
    }

    public TransformConfig setVersion(Version transformVersion) {
        this.transformVersion = transformVersion;
        return this;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    public TransformConfig setCreateTime(Instant createTime) {
        ExceptionsHelper.requireNonNull(createTime, TransformField.CREATE_TIME.getPreferredName());
        this.createTime = Instant.ofEpochMilli(createTime.toEpochMilli());
        return this;
    }

    public PivotConfig getPivotConfig() {
        return pivotConfig;
    }

    public LatestConfig getLatestConfig() {
        return latestConfig;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    public SettingsConfig getSettings() {
        return settings;
    }

    @Nullable
    public RetentionPolicyConfig getRetentionPolicyConfig() {
        return retentionPolicyConfig;
    }

    /**
     * Determines the minimum version of a cluster in multi-cluster setup that is needed to successfully run this transform config.
     *
     * @return version
     */
    public List<SourceDestValidation> getAdditionalSourceDestValidations() {
        if ((source.getRuntimeMappings() == null || source.getRuntimeMappings().isEmpty()) == false) {
            SourceDestValidation validation =
                new SourceDestValidator.RemoteClusterMinimumVersionValidation(
                    FIELD_CAPS_RUNTIME_MAPPINGS_INTRODUCED_VERSION, "source.runtime_mappings field was set");
            return Collections.singletonList(validation);
        } else {
            return Collections.emptyList();
        }
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        validationException = source.validate(validationException);
        validationException = dest.validate(validationException);
        validationException = settings.validate(validationException);
        if (pivotConfig != null) {
            validationException = pivotConfig.validate(validationException);
        }
        if (latestConfig != null) {
            validationException = latestConfig.validate(validationException);
        }
        if (retentionPolicyConfig != null) {
            validationException = retentionPolicyConfig.validate(validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(id);
        source.writeTo(out);
        dest.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeOptionalTimeValue(frequency);
        }
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        out.writeOptionalWriteable(pivotConfig);
        out.writeOptionalWriteable(latestConfig);
        out.writeOptionalString(description);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeOptionalNamedWriteable(syncConfig);
            out.writeOptionalInstant(createTime);
            if (transformVersion != null) {
                out.writeBoolean(true);
                Version.writeVersion(transformVersion, out);
            } else {
                out.writeBoolean(false);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
            settings.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_7_12_0)) {
            out.writeOptionalNamedWriteable(retentionPolicyConfig);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        final boolean excludeGenerated = params.paramAsBoolean(TransformField.EXCLUDE_GENERATED, false);
        final boolean forInternalStorage = params.paramAsBoolean(TransformField.FOR_INTERNAL_STORAGE, false);
        assert (forInternalStorage && excludeGenerated) == false
            : "unsupported behavior, exclude_generated is true and for_internal_storage is true";
        builder.startObject();
        builder.field(TransformField.ID.getPreferredName(), id);
        if (excludeGenerated == false) {
            if (headers.isEmpty() == false && forInternalStorage) {
                builder.field(HEADERS.getPreferredName(), headers);
            }
            if (transformVersion != null) {
                builder.field(TransformField.VERSION.getPreferredName(), transformVersion);
            }
            if (createTime != null) {
                builder.timeField(
                    TransformField.CREATE_TIME.getPreferredName(),
                    TransformField.CREATE_TIME.getPreferredName() + "_string",
                    createTime.toEpochMilli()
                );
            }
            if (forInternalStorage) {
                builder.field(TransformField.INDEX_DOC_TYPE.getPreferredName(), NAME);
            }
        }
        builder.field(TransformField.SOURCE.getPreferredName(), source, params);
        builder.field(TransformField.DESTINATION.getPreferredName(), dest);
        if (frequency != null) {
            builder.field(TransformField.FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        if (syncConfig != null) {
            builder.startObject(TransformField.SYNC.getPreferredName());
            builder.field(syncConfig.getWriteableName(), syncConfig);
            builder.endObject();
        }
        if (pivotConfig != null) {
            builder.field(Function.PIVOT.getParseField().getPreferredName(), pivotConfig);
        }
        if (latestConfig != null) {
            builder.field(Function.LATEST.getParseField().getPreferredName(), latestConfig);
        }
        if (description != null) {
            builder.field(TransformField.DESCRIPTION.getPreferredName(), description);
        }
        builder.field(TransformField.SETTINGS.getPreferredName(), settings);
        if (retentionPolicyConfig != null) {
            builder.startObject(TransformField.RETENTION_POLICY.getPreferredName());
            builder.field(retentionPolicyConfig.getWriteableName(), retentionPolicyConfig);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final TransformConfig that = (TransformConfig) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.source, that.source)
            && Objects.equals(this.dest, that.dest)
            && Objects.equals(this.frequency, that.frequency)
            && Objects.equals(this.syncConfig, that.syncConfig)
            && Objects.equals(this.headers, that.headers)
            && Objects.equals(this.pivotConfig, that.pivotConfig)
            && Objects.equals(this.latestConfig, that.latestConfig)
            && Objects.equals(this.description, that.description)
            && Objects.equals(this.settings, that.settings)
            && Objects.equals(this.retentionPolicyConfig, that.retentionPolicyConfig)
            && Objects.equals(this.createTime, that.createTime)
            && Objects.equals(this.transformVersion, that.transformVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            source,
            dest,
            frequency,
            syncConfig,
            headers,
            pivotConfig,
            latestConfig,
            description,
            settings,
            retentionPolicyConfig,
            createTime,
            transformVersion
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static TransformConfig fromXContent(final XContentParser parser, @Nullable final String optionalTransformId, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, optionalTransformId) : STRICT_PARSER.apply(parser, optionalTransformId);
    }

    /**
     * Rewrites the transform config according to the latest format.
     *
     * Operations cover:
     *
     *  - move deprecated settings to its new place
     *  - change configuration options so it stays compatible (given a newer version)
     *
     * @param transformConfig original config
     * @return a rewritten transform config if a rewrite was necessary, otherwise the given transformConfig
     */
    public static TransformConfig rewriteForUpdate(final TransformConfig transformConfig) {

        // quick check if a rewrite is required, if none found just return the original
        // a failing quick check, does not mean a rewrite is necessary
        if (transformConfig.getVersion() != null
            && transformConfig.getVersion().onOrAfter(Version.V_7_11_0)
            && (transformConfig.getPivotConfig() == null || transformConfig.getPivotConfig().getMaxPageSearchSize() == null)) {
            return transformConfig;
        }

        Builder builder = new Builder(transformConfig);

        // call apply rewrite without config, to only allow reading from the builder
        return applyRewriteForUpdate(builder);
    }

    private static TransformConfig applyRewriteForUpdate(Builder builder) {
        // 1. Move pivot.max_page_size_search to settings.max_page_size_search
        if (builder.getPivotConfig() != null && builder.getPivotConfig().getMaxPageSearchSize() != null) {

            // find maxPageSearchSize value
            Integer maxPageSearchSizeDeprecated = builder.getPivotConfig().getMaxPageSearchSize();
            Integer maxPageSearchSize = builder.getSettings().getMaxPageSearchSize() != null
                ? builder.getSettings().getMaxPageSearchSize()
                : maxPageSearchSizeDeprecated;

            // create a new pivot config but set maxPageSearchSize to null
            builder.setPivotConfig(
                new PivotConfig(builder.getPivotConfig().getGroupConfig(), builder.getPivotConfig().getAggregationConfig(), null)
            );
            // create new settings with maxPageSearchSize
            builder.setSettings(
                new SettingsConfig(
                    maxPageSearchSize,
                    builder.getSettings().getDocsPerSecond(),
                    builder.getSettings().getDatesAsEpochMillis(),
                    builder.getSettings().getInterimResults()
                )
            );
        }

        // 2. set dates_as_epoch_millis to true for transforms < 7.11 to keep BWC
        if (builder.getVersion() != null && builder.getVersion().before(Version.V_7_11_0)) {
            builder.setSettings(
                new SettingsConfig(
                    builder.getSettings().getMaxPageSearchSize(),
                    builder.getSettings().getDocsPerSecond(),
                    true,
                    builder.getSettings().getInterimResults())
            );
        }

        // 3. set interim_results to true for transforms < 7.15 to keep BWC
        if (builder.getVersion() != null && builder.getVersion().before(Version.CURRENT)) {  // TODO: 7.15
            builder.setSettings(
                new SettingsConfig(
                    builder.getSettings().getMaxPageSearchSize(),
                    builder.getSettings().getDocsPerSecond(),
                    builder.getSettings().getDatesAsEpochMillis(),
                    true)
            );
        }

        return builder.setVersion(Version.CURRENT).build();
    }

    public static class Builder {
        private String id;
        private SourceConfig source;
        private DestConfig dest;
        private TimeValue frequency;
        private SyncConfig syncConfig;
        private String description;
        private Map<String, String> headers;
        private Version transformVersion;
        private Instant createTime;
        private PivotConfig pivotConfig;
        private LatestConfig latestConfig;
        private SettingsConfig settings;
        private RetentionPolicyConfig retentionPolicyConfig;

        public Builder() {}

        public Builder(TransformConfig config) {
            this.id = config.id;
            this.source = config.source;
            this.dest = config.dest;
            this.frequency = config.frequency;
            this.syncConfig = config.syncConfig;
            this.description = config.description;
            this.transformVersion = config.transformVersion;
            this.createTime = config.createTime;
            this.pivotConfig = config.pivotConfig;
            this.latestConfig = config.latestConfig;
            this.settings = config.settings;
            this.retentionPolicyConfig = config.retentionPolicyConfig;
        }

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        String getId() {
            return id;
        }

        public Builder setSource(SourceConfig source) {
            this.source = source;
            return this;
        }

        SourceConfig getSource() {
            return source;
        }

        public Builder setDest(DestConfig dest) {
            this.dest = dest;
            return this;
        }

        DestConfig getDest() {
            return dest;
        }

        public Builder setFrequency(TimeValue frequency) {
            this.frequency = frequency;
            return this;
        }

        TimeValue getFrequency() {
            return frequency;
        }

        public Builder setSyncConfig(SyncConfig syncConfig) {
            this.syncConfig = syncConfig;
            return this;
        }

        SyncConfig getSyncConfig() {
            return syncConfig;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        String getDescription() {
            return description;
        }

        public Builder setSettings(SettingsConfig settings) {
            this.settings = settings;
            return this;
        }

        SettingsConfig getSettings() {
            return settings;
        }

        public Builder setHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Map<String, String> getHeaders() {
            return headers;
        }

        public Builder setPivotConfig(PivotConfig pivotConfig) {
            this.pivotConfig = pivotConfig;
            return this;
        }

        PivotConfig getPivotConfig() {
            return pivotConfig;
        }

        public Builder setLatestConfig(LatestConfig latestConfig) {
            this.latestConfig = latestConfig;
            return this;
        }

        public LatestConfig getLatestConfig() {
            return latestConfig;
        }

        Builder setVersion(Version version) {
            this.transformVersion = version;
            return this;
        }

        Version getVersion() {
            return transformVersion;
        }

        public Builder setRetentionPolicyConfig(RetentionPolicyConfig retentionPolicyConfig) {
            this.retentionPolicyConfig = retentionPolicyConfig;
            return this;
        }

        public TransformConfig build() {
            return new TransformConfig(
                id,
                source,
                dest,
                frequency,
                syncConfig,
                headers,
                pivotConfig,
                latestConfig,
                description,
                settings,
                retentionPolicyConfig,
                createTime,
                transformVersion == null ? null : transformVersion.toString()
            );
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final TransformConfig.Builder that = (TransformConfig.Builder) other;

            return Objects.equals(this.id, that.id)
                && Objects.equals(this.source, that.source)
                && Objects.equals(this.dest, that.dest)
                && Objects.equals(this.frequency, that.frequency)
                && Objects.equals(this.syncConfig, that.syncConfig)
                && Objects.equals(this.headers, that.headers)
                && Objects.equals(this.pivotConfig, that.pivotConfig)
                && Objects.equals(this.latestConfig, that.latestConfig)
                && Objects.equals(this.description, that.description)
                && Objects.equals(this.settings, that.settings)
                && Objects.equals(this.retentionPolicyConfig, that.retentionPolicyConfig)
                && Objects.equals(this.createTime, that.createTime)
                && Objects.equals(this.transformVersion, that.transformVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                id,
                source,
                dest,
                frequency,
                syncConfig,
                headers,
                pivotConfig,
                latestConfig,
                description,
                settings,
                retentionPolicyConfig,
                createTime,
                transformVersion
            );
        }
    }
}
