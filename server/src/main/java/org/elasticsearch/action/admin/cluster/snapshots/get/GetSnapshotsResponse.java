/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Get snapshots response
 */
public class GetSnapshotsResponse extends ActionResponse implements ToXContentObject {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetSnapshotsResponse, Void> GET_SNAPSHOT_PARSER = new ConstructingObjectParser<>(
        GetSnapshotsResponse.class.getName(),
        true,
        (args) -> new GetSnapshotsResponse((List<SnapshotInfo>) args[0], (Map<String, ElasticsearchException>) args[1], (String) args[2])
    );

    static {
        GET_SNAPSHOT_PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> SnapshotInfo.SNAPSHOT_INFO_PARSER.apply(p, c).build(),
            new ParseField("snapshots")
        );
        GET_SNAPSHOT_PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> p.map(HashMap::new, ElasticsearchException::fromXContent),
            new ParseField("failures")
        );
        GET_SNAPSHOT_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("next"));
    }

    private final List<SnapshotInfo> snapshots;

    private final Map<String, ElasticsearchException> failures;

    @Nullable
    private final String next;

    public GetSnapshotsResponse(List<SnapshotInfo> snapshots, Map<String, ElasticsearchException> failures, @Nullable String next) {
        this.snapshots = List.copyOf(snapshots);
        this.failures = failures == null ? Map.of() : Map.copyOf(failures);
        this.next = next;
    }

    public GetSnapshotsResponse(StreamInput in) throws IOException {
        this.snapshots = in.readList(SnapshotInfo::readFrom);
        if (in.getVersion().onOrAfter(GetSnapshotsRequest.MULTIPLE_REPOSITORIES_SUPPORT_ADDED)) {
            final Map<String, ElasticsearchException> failedResponses = in.readMap(StreamInput::readString, StreamInput::readException);
            this.failures = Collections.unmodifiableMap(failedResponses);
            this.next = in.readOptionalString();
        } else {
            this.failures = Collections.emptyMap();
            this.next = null;
        }
    }

    /**
     * Returns the list of snapshots
     *
     * @return the list of snapshots
     */
    public List<SnapshotInfo> getSnapshots() {
        return snapshots;
    }

    /**
     * Returns a map of repository name to {@link ElasticsearchException} for each unsuccessful response.
     */
    public Map<String, ElasticsearchException> getFailures() {
        return failures;
    }

    @Nullable
    public String next() {
        return next;
    }

    /**
     * Returns true if there is a least one failed response.
     */
    public boolean isFailed() {
        return failures.isEmpty() == false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(snapshots);
        if (out.getVersion().onOrAfter(GetSnapshotsRequest.MULTIPLE_REPOSITORIES_SUPPORT_ADDED)) {
            out.writeMap(failures, StreamOutput::writeString, StreamOutput::writeException);
            out.writeOptionalString(next);
        } else {
            if (failures.isEmpty() == false) {
                assert false : "transport action should have thrown directly for old version but saw " + failures;
                throw failures.values().iterator().next();
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("snapshots");
        for (SnapshotInfo snapshotInfo : snapshots) {
            snapshotInfo.toXContent(builder, params);
        }
        builder.endArray();
        if (failures.isEmpty() == false) {
            builder.startObject("failures");
            for (Map.Entry<String, ElasticsearchException> error : failures.entrySet()) {
                builder.field(error.getKey(), (b, pa) -> {
                    b.startObject();
                    error.getValue().toXContent(b, pa);
                    b.endObject();
                    return b;
                });
            }
            builder.endObject();
        }
        if (next != null) {
            builder.field("next", next);
        }
        builder.endObject();
        return builder;
    }

    public static GetSnapshotsResponse fromXContent(XContentParser parser) throws IOException {
        return GET_SNAPSHOT_PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSnapshotsResponse that = (GetSnapshotsResponse) o;
        return Objects.equals(snapshots, that.snapshots) && Objects.equals(failures, that.failures) && Objects.equals(next, that.next);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshots, failures, next);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
