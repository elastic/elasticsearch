/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.scheduler.Cron;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    private final String id;
    private final String name;
    private final String schedule;
    private final String repository;
    private final Map<String, Object> configuration;
    private final SnapshotRetentionConfiguration retentionPolicy;

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField SCHEDULE = new ParseField("schedule");
    private static final ParseField REPOSITORY = new ParseField("repository");
    private static final ParseField CONFIG = new ParseField("config");
    private static final ParseField RETENTION = new ParseField("retention");
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
            return new SnapshotLifecyclePolicy(id, name, schedule, repo, config, retention);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SCHEDULE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), CONFIG);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotRetentionConfiguration::parse, RETENTION);
    }

    public SnapshotLifecyclePolicy(
        final String id,
        final String name,
        final String schedule,
        final String repository,
        @Nullable final Map<String, Object> configuration,
        @Nullable final SnapshotRetentionConfiguration retentionPolicy
    ) {
        this.id = Objects.requireNonNull(id, "policy id is required");
        this.name = Objects.requireNonNull(name, "policy snapshot name is required");
        this.schedule = Objects.requireNonNull(schedule, "policy schedule is required");
        this.repository = Objects.requireNonNull(repository, "policy snapshot repository is required");
        this.configuration = configuration;
        this.retentionPolicy = retentionPolicy;
    }

    public SnapshotLifecyclePolicy(StreamInput in) throws IOException {
        this.id = in.readString();
        this.name = in.readString();
        this.schedule = in.readString();
        this.repository = in.readString();
        this.configuration = in.readMap();
        this.retentionPolicy = in.readOptionalWriteable(SnapshotRetentionConfiguration::new);
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

    public long calculateNextExecution() {
        final Cron scheduleEvaluator = new Cron(this.schedule);
        return scheduleEvaluator.getNextValidTimeAfter(System.currentTimeMillis());
    }

    /**
     * Calculate the difference between the next two valid times after now for the schedule.
     * <p>
     * In ordinary cases, this can be treated as the interval between executions of the schedule (for schedules like 'twice an hour' or
     * 'every five minutes').
     *
     * @return a {@link TimeValue} representing the difference between the next two valid times after now, or {@link TimeValue#MINUS_ONE}
     *         if either of the next two times after now is unsupported according to @{@link Cron#getNextValidTimeAfter(long)}
     */
    public TimeValue calculateNextInterval() {
        final Cron scheduleEvaluator = new Cron(this.schedule);
        long next1 = scheduleEvaluator.getNextValidTimeAfter(System.currentTimeMillis());
        long next2 = scheduleEvaluator.getNextValidTimeAfter(next1);
        if (next1 > 0 && next2 > 0) {
            return TimeValue.timeValueMillis(next2 - next1);
        } else {
            return TimeValue.MINUS_ONE;
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
        if (Strings.hasText(schedule) == false) {
            err.addValidationError("invalid schedule [" + schedule + "]: must not be empty");
        } else {
            try {
                new Cron(schedule);
            } catch (IllegalArgumentException e) {
                err.addValidationError("invalid schedule: " + ExceptionsHelper.unwrapCause(e).getMessage());
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

        return err.validationErrors().size() == 0 ? null : err;
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
     */
    public CreateSnapshotRequest toRequest() {
        CreateSnapshotRequest req = new CreateSnapshotRequest(repository, generateSnapshotName(this.name));
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
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, schedule, repository, configuration, retentionPolicy);
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
            && Objects.equals(retentionPolicy, other.retentionPolicy);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
