/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Contains information about the status of a single component (e.g. `shard_migration`, `persistent_tasks`) of the node shutdown process.
 */
public class NodeShutdownComponentStatus extends AbstractDiffable<NodeShutdownComponentStatus> implements ToXContentFragment {
    private final SingleNodeShutdownMetadata.Status status;
    @Nullable private final Long startedAtMillis;
    @Nullable private final String errorMessage;

    private static final ParseField STATUS_FIELD = new ParseField("status");
    private static final ParseField TIME_STARTED_FIELD = new ParseField("time_started_millis");
    private static final ParseField ERROR_FIELD = new ParseField("error");
    private static final ConstructingObjectParser<NodeShutdownComponentStatus, Void> PARSER = new ConstructingObjectParser<>(
        "node_shutdown_component",
        a -> new NodeShutdownComponentStatus(SingleNodeShutdownMetadata.Status.valueOf((String) a[0]), (Long) a[1], (String) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), STATUS_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TIME_STARTED_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ERROR_FIELD);
    }

    public static NodeShutdownComponentStatus parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public NodeShutdownComponentStatus(
        SingleNodeShutdownMetadata.Status status,
        @Nullable Long startedAtMillis,
        @Nullable String errorMessage) {
        this.status = status;
        this.startedAtMillis = startedAtMillis;
        this.errorMessage = errorMessage;
    }

    public NodeShutdownComponentStatus(StreamInput in) throws IOException {
        this.status = in.readEnum(SingleNodeShutdownMetadata.Status.class);
        this.startedAtMillis = in.readOptionalVLong();
        this.errorMessage = in.readOptionalString();
    }

    /**
     * @return The overall status of this component.
     */
    public SingleNodeShutdownMetadata.Status getStatus() {
        return status;
    }

    /**
     * @return The timestamp this component started shutting down. Null if the component has not yet started shutting down.
     */
    @Nullable public Long getStartedAtMillis() {
        return startedAtMillis;
    }

    /**
     * @return The error message this component encountered while trying to shut down, if any. Null if no errors have been encountered.
     */
    @Nullable public String getErrorMessage() {
        return errorMessage;
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(STATUS_FIELD.getPreferredName(), status);
            if (startedAtMillis != null) {
                builder.timeField(TIME_STARTED_FIELD.getPreferredName(), "time_started", startedAtMillis);
            }
            if (errorMessage != null) {
                builder.field(ERROR_FIELD.getPreferredName(), errorMessage);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
        out.writeOptionalVLong(startedAtMillis);
        out.writeOptionalString(errorMessage);
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if ((o instanceof NodeShutdownComponentStatus) == false)
            return false;
        NodeShutdownComponentStatus that = (NodeShutdownComponentStatus) o;
        return getStatus() == that.getStatus() && Objects.equals(getStartedAtMillis(), that.getStartedAtMillis()) && Objects.equals(
            getErrorMessage(),
            that.getErrorMessage());
    }

    @Override public int hashCode() {
        return Objects.hash(getStatus(), getStartedAtMillis(), getErrorMessage());
    }
}
