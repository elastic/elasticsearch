/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.core.scheduler.Cron;
import org.elasticsearch.xpack.core.scheduler.CronSchedule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.MAX_INDEX_NAME_BYTES;
import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.generateSnapshotName;
import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.validateGeneratedSnapshotName;

/**
 * A {@code SnapshotLifecyclePolicy} is a policy for the cluster including a schedule of when a
 * snapshot should be triggered, what the snapshot should be named, what repository it should go
 * to, and the configuration for the snapshot itself.
 */
public class SnapshotLifecyclePolicy implements SimpleDiffable<SnapshotLifecyclePolicy>, Writeable, ToXContentObject {

    private static final TransportVersion SLM_ENCRYPTED_DATA = TransportVersion.fromName("snapshot_encrypted_data");

    private final String id;
    private final String name;
    private final String schedule;
    private final String repository;
    private final Map<String, Object> configuration;
    private final SnapshotRetentionConfiguration retentionPolicy;
    private final boolean isCronSchedule;
    private final TimeValue unhealthyIfNoSnapshotWithin;
    /** Password-encrypted with the cluster PEK; null if no encryption password was configured. */
    @Nullable
    private final EncryptedData encryptedPassword;
    /** User-supplied identifier of the password; always set when {@link #encryptedPassword} is, never without it. */
    @Nullable
    private final String encryptedPasswordId;

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField SCHEDULE = new ParseField("schedule");
    private static final ParseField REPOSITORY = new ParseField("repository");
    private static final ParseField CONFIG = new ParseField("config");
    private static final ParseField RETENTION = new ParseField("retention");
    private static final ParseField UNHEALTHY_IF_NO_SNAPSHOT_WITHIN = new ParseField("unhealthy_if_no_snapshot_within");
    private static final ParseField ENCRYPTED_DATA = new ParseField("encrypted_data");
    private static final ParseField ENCRYPTED_DATA_TYPE = new ParseField("type");
    private static final ParseField ENCRYPTED_DATA_PASSWORD = new ParseField("password");
    private static final ParseField ENCRYPTED_DATA_PASSWORD_ID = new ParseField("password_id");
    private static final String ENCRYPTED_DATA_TYPE_PASSWORD = "password";
    private static final String METADATA_FIELD_NAME = "metadata";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SnapshotLifecyclePolicy, String> PARSER = new ConstructingObjectParser<>(
        "snapshot_lifecycle",
        true,
        (a, id) -> {
            String name = (String) a[0];
            String schedule = (String) a[1];
            String repo = (String) a[2];
            Map<String, Object> config = (Map<String, Object>) a[3];
            SnapshotRetentionConfiguration retention = (SnapshotRetentionConfiguration) a[4];
            TimeValue unhealthyIfNoSnapshotWithin = (TimeValue) a[5];
            PolicyEncryptedData encryptedData = (PolicyEncryptedData) a[6];
            return new SnapshotLifecyclePolicy(
                id,
                name,
                schedule,
                repo,
                config,
                retention,
                unhealthyIfNoSnapshotWithin,
                encryptedData == null ? null : encryptedData.password(),
                encryptedData == null ? null : encryptedData.passwordId()
            );
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SCHEDULE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), CONFIG);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotRetentionConfiguration::parse, RETENTION);
        PARSER.declareString(
            ConstructingObjectParser.optionalConstructorArg(),
            value -> TimeValue.parseTimeValue(value, UNHEALTHY_IF_NO_SNAPSHOT_WITHIN.getPreferredName()),
            UNHEALTHY_IF_NO_SNAPSHOT_WITHIN
        );
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), PolicyEncryptedData.PARSER, ENCRYPTED_DATA);
    }

    /** The persisted {@code encrypted_data} object: PEK-encrypted password plus its plaintext id. */
    private record PolicyEncryptedData(@Nullable EncryptedData password, @Nullable String passwordId) {
        private static final ConstructingObjectParser<PolicyEncryptedData, String> PARSER = new ConstructingObjectParser<>(
            "slm_encrypted_data",
            true,
            a -> new PolicyEncryptedData((EncryptedData) a[1], (String) a[2])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), ENCRYPTED_DATA_TYPE);
            PARSER.declareObject(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> EncryptedData.fromXContent(p),
                ENCRYPTED_DATA_PASSWORD
            );
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ENCRYPTED_DATA_PASSWORD_ID);
        }
    }

    private static final TransportVersion SLM_UNHEALTHY_IF_NO_SNAPSHOT_WITHIN = TransportVersion.fromName(
        "slm_unhealthy_if_no_snapshot_within"
    );

    public SnapshotLifecyclePolicy(
        final String id,
        final String name,
        final String schedule,
        final String repository,
        @Nullable final Map<String, Object> configuration,
        @Nullable final SnapshotRetentionConfiguration retentionPolicy
    ) {
        this(id, name, schedule, repository, configuration, retentionPolicy, null, null, null);
    }

    public SnapshotLifecyclePolicy(
        final String id,
        final String name,
        final String schedule,
        final String repository,
        @Nullable final Map<String, Object> configuration,
        @Nullable final SnapshotRetentionConfiguration retentionPolicy,
        @Nullable final TimeValue unhealthyIfNoSnapshotWithin
    ) {
        this(id, name, schedule, repository, configuration, retentionPolicy, unhealthyIfNoSnapshotWithin, null, null);
    }

    public SnapshotLifecyclePolicy(
        final String id,
        final String name,
        final String schedule,
        final String repository,
        @Nullable final Map<String, Object> configuration,
        @Nullable final SnapshotRetentionConfiguration retentionPolicy,
        @Nullable final TimeValue unhealthyIfNoSnapshotWithin,
        @Nullable final EncryptedData encryptedPassword,
        @Nullable final String encryptedPasswordId
    ) {
        this.id = Objects.requireNonNull(id, "policy id is required");
        this.name = Objects.requireNonNull(name, "policy snapshot name is required");
        this.schedule = Objects.requireNonNull(schedule, "policy schedule is required");
        this.repository = Objects.requireNonNull(repository, "policy snapshot repository is required");
        this.configuration = configuration;
        this.retentionPolicy = retentionPolicy;
        this.unhealthyIfNoSnapshotWithin = unhealthyIfNoSnapshotWithin;
        this.isCronSchedule = isCronSchedule(schedule);
        this.encryptedPassword = encryptedPassword;
        this.encryptedPasswordId = encryptedPasswordId;
    }

    public SnapshotLifecyclePolicy(StreamInput in) throws IOException {
        this.id = in.readString();
        this.name = in.readString();
        this.schedule = in.readString();
        this.repository = in.readString();
        this.configuration = in.readGenericMap();
        this.retentionPolicy = in.readOptionalWriteable(SnapshotRetentionConfiguration::new);
        this.unhealthyIfNoSnapshotWithin = in.getTransportVersion().supports(SLM_UNHEALTHY_IF_NO_SNAPSHOT_WITHIN)
            ? in.readOptionalTimeValue()
            : null;
        final boolean supportsEncryptedPassword = in.getTransportVersion().supports(SLM_ENCRYPTED_DATA);
        this.encryptedPassword = supportsEncryptedPassword ? in.readOptionalWriteable(EncryptedData::new) : null;
        this.encryptedPasswordId = supportsEncryptedPassword ? in.readOptionalString() : null;
        this.isCronSchedule = isCronSchedule(schedule);
    }

    public String getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public String getSchedule() {
        return this.schedule;
    }

    public String getRepository() {
        return this.repository;
    }

    @Nullable
    public Map<String, Object> getConfig() {
        return this.configuration;
    }

    @Nullable
    public SnapshotRetentionConfiguration getRetentionPolicy() {
        return this.retentionPolicy;
    }

    @Nullable
    public TimeValue getUnhealthyIfNoSnapshotWithin() {
        return this.unhealthyIfNoSnapshotWithin;
    }

    boolean isCronSchedule() {
        return this.isCronSchedule;
    }

    /**
     * @return whether `schedule` is a cron expression
     */
    static boolean isCronSchedule(String schedule) {
        try {
            new Cron(schedule);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * @return whether `schedule` is an interval time unit expression
     */
    public static boolean isIntervalSchedule(String schedule) {
        try {
            TimeValue.parseTimeValue(schedule, "schedule");
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public long calculateNextExecution(long modifiedDate, Clock clock) {
        if (isCronSchedule()) {
            final Cron scheduleEvaluator = new Cron(this.schedule);
            return scheduleEvaluator.getNextValidTimeAfter(clock.millis());
        } else {
            final TimeValue interval = TimeValue.parseTimeValue(this.schedule, SCHEDULE.getPreferredName());
            final TimeValueSchedule timeValueSchedule = new TimeValueSchedule(interval);
            return timeValueSchedule.nextScheduledTimeAfter(modifiedDate, clock.millis());
        }
    }

    /**
     * Calculate the difference between the next two valid times after now for the schedule.
     * <p>
     * In ordinary cases, this can be treated as the interval between executions of the schedule (for schedules like 'twice an hour' or
     * 'every five minutes').
     * @param clock a clock to provide current time
     * @return a {@link TimeValue} representing the difference between the next two valid times after now, or {@link TimeValue#MINUS_ONE}
     *         if either of the next two times after now is unsupported according to @{@link Cron#getNextValidTimeAfter(long)}
     */
    public TimeValue calculateNextInterval(Clock clock) {
        if (isCronSchedule() == false) {
            return TimeValue.parseTimeValue(schedule, SCHEDULE.getPreferredName());
        }

        final Cron scheduleEvaluator = new Cron(this.schedule);
        long next1 = scheduleEvaluator.getNextValidTimeAfter(clock.millis());
        long next2 = scheduleEvaluator.getNextValidTimeAfter(next1);
        if (next1 > 0 && next2 > 0) {
            return TimeValue.timeValueMillis(next2 - next1);
        } else {
            return TimeValue.MINUS_ONE;
        }
    }

    public SchedulerEngine.Job buildSchedulerJob(String jobId, long modifiedDate) {
        if (isCronSchedule()) {
            return new SchedulerEngine.Job(jobId, new CronSchedule(schedule));
        } else {
            TimeValue timeValue = TimeValue.parseTimeValue(schedule, "schedule");
            return new SchedulerEngine.Job(jobId, new TimeValueSchedule(timeValue), modifiedDate);
        }
    }

    public ActionRequestValidationException validate() {
        ActionRequestValidationException err = new ActionRequestValidationException();

        // ID validation
        if (Strings.validFileName(id) == false) {
            err.addValidationError(
                "invalid policy id [" + id + "]: must not contain the following characters " + Strings.INVALID_FILENAME_CHARS
            );
        }
        if (id.charAt(0) == '_') {
            err.addValidationError("invalid policy id [" + id + "]: must not start with '_'");
        }
        int byteCount = id.getBytes(StandardCharsets.UTF_8).length;
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            err.addValidationError(
                "invalid policy id [" + id + "]: name is too long, (" + byteCount + " > " + MAX_INDEX_NAME_BYTES + " bytes)"
            );
        }

        // Snapshot name validation
        // We generate a snapshot name here to make sure it validates after applying date math
        final String snapshotName = generateSnapshotName(this.name);
        ActionRequestValidationException nameValidationErrors = validateGeneratedSnapshotName(name, snapshotName);
        if (nameValidationErrors != null) {
            err.addValidationErrors(nameValidationErrors.validationErrors());
        }

        // Schedule validation
        // n.b. there's more validation beyond this in SnapshotLifecycleService#validateMinimumInterval
        boolean canValidateUnhealthyIfNoSnapshotWithin = false;    // true if schedule is syntactically valid
        if (Strings.hasText(schedule) == false) {
            err.addValidationError("invalid schedule [" + schedule + "]: must not be empty");
        } else {
            try {
                var intervalTimeValue = TimeValue.parseTimeValue(schedule, SCHEDULE.getPreferredName());
                if (intervalTimeValue.millis() == 0) {
                    err.addValidationError("invalid schedule [" + schedule + "]: time unit must be at least 1 millisecond");
                }
                canValidateUnhealthyIfNoSnapshotWithin = true;
            } catch (IllegalArgumentException e1) {
                if (isCronSchedule(schedule) == false) {
                    err.addValidationError("invalid schedule [" + schedule + "]: must be a valid cron expression or time unit");
                } else {
                    canValidateUnhealthyIfNoSnapshotWithin = true;
                }
            }
        }

        // validate unhealthyIfNoSnapshotWithin if schedule is syntactically valid
        if (canValidateUnhealthyIfNoSnapshotWithin) {
            TimeValue snapshotInterval = calculateNextInterval(Clock.systemUTC());
            if (unhealthyIfNoSnapshotWithin != null
                && snapshotInterval.duration() > 0
                && unhealthyIfNoSnapshotWithin.compareTo(snapshotInterval) < 0) {
                err.addValidationError(
                    "invalid unhealthy_if_no_snapshot_within ["
                        + unhealthyIfNoSnapshotWithin.getStringRep()
                        + "]: "
                        + "time is too short, expecting at least more than the interval between snapshots ["
                        + snapshotInterval.toHumanReadableString(2)
                        + "] for schedule ["
                        + schedule
                        + "]"
                );
            }
        }

        if (configuration != null && configuration.containsKey(METADATA_FIELD_NAME)) {
            if (configuration.get(METADATA_FIELD_NAME) instanceof Map == false) {
                err.addValidationError(
                    "invalid configuration."
                        + METADATA_FIELD_NAME
                        + " ["
                        + configuration.get(METADATA_FIELD_NAME)
                        + "]: must be an object if present"
                );
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> metadata = (Map<String, Object>) configuration.get(METADATA_FIELD_NAME);
                if (metadata.containsKey(SnapshotsService.POLICY_ID_METADATA_FIELD)) {
                    err.addValidationError(
                        "invalid configuration."
                            + METADATA_FIELD_NAME
                            + ": field name ["
                            + SnapshotsService.POLICY_ID_METADATA_FIELD
                            + "] is reserved and will be added automatically"
                    );
                } else {
                    Map<String, Object> metadataWithPolicyField = addPolicyNameToMetadata(metadata);
                    int serializedSizeOriginal = CreateSnapshotRequest.metadataSize(metadata);
                    int serializedSizeWithMetadata = CreateSnapshotRequest.metadataSize(metadataWithPolicyField);
                    int policyNameAddedBytes = serializedSizeWithMetadata - serializedSizeOriginal;
                    if (serializedSizeWithMetadata > CreateSnapshotRequest.MAXIMUM_METADATA_BYTES) {
                        err.addValidationError(
                            "invalid configuration."
                                + METADATA_FIELD_NAME
                                + ": must be smaller than ["
                                + (CreateSnapshotRequest.MAXIMUM_METADATA_BYTES - policyNameAddedBytes)
                                + "] bytes, but is ["
                                + serializedSizeOriginal
                                + "] bytes"
                        );
                    }
                }
            }
        }

        // Repository validation, validation of whether the repository actually exists happens
        // elsewhere as it requires cluster state
        if (Strings.hasText(repository) == false) {
            err.addValidationError("invalid repository name [" + repository + "]: cannot be empty");
        }

        return err.validationErrors().isEmpty() ? null : err;
    }

    private Map<String, Object> addPolicyNameToMetadata(final Map<String, Object> metadata) {
        Map<String, Object> newMetadata;
        if (metadata == null) {
            newMetadata = new HashMap<>();
        } else {
            newMetadata = new HashMap<>(metadata);
        }
        newMetadata.put(SnapshotsService.POLICY_ID_METADATA_FIELD, this.id);
        return newMetadata;
    }

    /**
     * Generate a new create snapshot request from this policy. The name of the snapshot is
     * generated at this time based on any date math expressions in the "name" field.
     *
     * <p>The request does not carry the {@link #getEncryptedPassword() policy password}: decrypting it requires the
     * encryption service, which this class (living in x-pack core) has no access to. The SLM plugin sets the plaintext
     * password on the request when it executes the policy.
     */
    public CreateSnapshotRequest toRequest(TimeValue masterNodeTimeout) {
        CreateSnapshotRequest req = new CreateSnapshotRequest(masterNodeTimeout, repository, generateSnapshotName(this.name));
        Map<String, Object> mergedConfiguration = configuration == null ? new HashMap<>() : new HashMap<>(configuration);
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) mergedConfiguration.get("metadata");
        Map<String, Object> metadataWithAddedPolicyName = addPolicyNameToMetadata(metadata);
        mergedConfiguration.put("metadata", metadataWithAddedPolicyName);
        req.source(mergedConfiguration);
        req.waitForCompletion(true);
        return req;
    }

    public static SnapshotLifecyclePolicy parse(XContentParser parser, String id) {
        return PARSER.apply(parser, id);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeString(this.name);
        out.writeString(this.schedule);
        out.writeString(this.repository);
        out.writeGenericMap(this.configuration);
        out.writeOptionalWriteable(this.retentionPolicy);
        if (out.getTransportVersion().supports(SLM_UNHEALTHY_IF_NO_SNAPSHOT_WITHIN)) {
            out.writeOptionalTimeValue(this.unhealthyIfNoSnapshotWithin);
        }
        if (out.getTransportVersion().supports(SLM_ENCRYPTED_DATA)) {
            out.writeOptionalWriteable(this.encryptedPassword);
            out.writeOptionalString(this.encryptedPasswordId);
        }
    }

    /**
     * Returns a copy of this policy with the given {@code encryptedPassword}. Clearing the password ({@code null})
     * also clears the password id; replacing it keeps the id, since the id names the underlying plaintext password.
     */
    public SnapshotLifecyclePolicy withEncryptedPassword(@Nullable EncryptedData encryptedPassword) {
        return new SnapshotLifecyclePolicy(
            id,
            name,
            schedule,
            repository,
            configuration,
            retentionPolicy,
            unhealthyIfNoSnapshotWithin,
            encryptedPassword,
            encryptedPassword == null ? null : encryptedPasswordId
        );
    }

    /** Returns the PEK-encrypted snapshot password, or {@code null} if none was configured. */
    @Nullable
    public EncryptedData getEncryptedPassword() {
        return encryptedPassword;
    }

    /** Returns the id of the encrypted snapshot password, or {@code null} if no password was configured. */
    @Nullable
    public String getEncryptedPasswordId() {
        return encryptedPasswordId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), this.name);
        builder.field(SCHEDULE.getPreferredName(), this.schedule);
        builder.field(REPOSITORY.getPreferredName(), this.repository);
        if (this.configuration != null) {
            builder.field(CONFIG.getPreferredName(), this.configuration);
        }
        if (this.retentionPolicy != null) {
            builder.field(RETENTION.getPreferredName(), this.retentionPolicy);
        }
        if (this.unhealthyIfNoSnapshotWithin != null) {
            builder.field(UNHEALTHY_IF_NO_SNAPSHOT_WITHIN.getPreferredName(), this.unhealthyIfNoSnapshotWithin);
        }
        // the encrypted password is persisted (gateway state, snapshot global state) but never exposed via the API;
        // the type and password id are not sensitive and are returned by the API as well
        if (this.encryptedPassword != null) {
            builder.startObject(ENCRYPTED_DATA.getPreferredName());
            builder.field(ENCRYPTED_DATA_TYPE.getPreferredName(), ENCRYPTED_DATA_TYPE_PASSWORD);
            if (Metadata.XContentContext.from(params) != Metadata.XContentContext.API) {
                builder.field(ENCRYPTED_DATA_PASSWORD.getPreferredName(), this.encryptedPassword);
            }
            if (this.encryptedPasswordId != null) {
                builder.field(ENCRYPTED_DATA_PASSWORD_ID.getPreferredName(), this.encryptedPasswordId);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            name,
            schedule,
            repository,
            configuration,
            retentionPolicy,
            unhealthyIfNoSnapshotWithin,
            encryptedPassword,
            encryptedPasswordId
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != getClass()) {
            return false;
        }
        SnapshotLifecyclePolicy other = (SnapshotLifecyclePolicy) obj;
        return Objects.equals(id, other.id)
            && Objects.equals(name, other.name)
            && Objects.equals(schedule, other.schedule)
            && Objects.equals(repository, other.repository)
            && Objects.equals(configuration, other.configuration)
            && Objects.equals(retentionPolicy, other.retentionPolicy)
            && Objects.equals(unhealthyIfNoSnapshotWithin, other.unhealthyIfNoSnapshotWithin)
            && Objects.equals(encryptedPassword, other.encryptedPassword)
            && Objects.equals(encryptedPasswordId, other.encryptedPasswordId);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
