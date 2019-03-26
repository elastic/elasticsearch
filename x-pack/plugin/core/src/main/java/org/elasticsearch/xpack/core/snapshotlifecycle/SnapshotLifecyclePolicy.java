/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A {@code SnapshotLifecyclePolicy} is a policy for the cluster including a schedule of when a
 * snapshot should be triggered, what the snapshot should be named, what repository it should go
 * to, and the configuration for the snapshot itself.
 */
public class SnapshotLifecyclePolicy extends AbstractDiffable<SnapshotLifecyclePolicy>
    implements Writeable, Diffable<SnapshotLifecyclePolicy>, ToXContentObject {

    private final String id;
    private final String name;
    private final String schedule;
    private final String repository;
    private final Map<String, Object> configuration;

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField SCHEDULE = new ParseField("schedule");
    private static final ParseField REPOSITORY = new ParseField("repository");
    private static final ParseField CONFIG = new ParseField("config");
    private static final IndexNameExpressionResolver.DateMathExpressionResolver DATE_MATH_RESOLVER =
        new IndexNameExpressionResolver.DateMathExpressionResolver();

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SnapshotLifecyclePolicy, String> PARSER =
        new ConstructingObjectParser<>("snapshot_lifecycle", true,
            (a, id) -> {
                String name = (String) a[0];
                String schedule = (String) a[1];
                String repo = (String) a[2];
                Map<String, Object> config = (Map<String, Object>) a[3];
                return new SnapshotLifecyclePolicy(id, name, schedule, repo, config);
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SCHEDULE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), CONFIG);
    }

    public SnapshotLifecyclePolicy(final String id, final String name, final String schedule,
                                   final String repository, Map<String, Object> configuration) {
        this.id = id;
        this.name = name;
        this.schedule = schedule;
        this.repository = repository;
        this.configuration = configuration;
    }

    public SnapshotLifecyclePolicy(StreamInput in) throws IOException {
        this.id = in.readString();
        this.name = in.readString();
        this.schedule = in.readString();
        this.repository = in.readString();
        this.configuration = in.readMap();
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

    public Map<String, Object> getConfig() {
        return this.configuration;
    }

    public ValidationException validate() {
        // TODO: implement validation
        return null;
    }

    /**
     * Since snapshots need to be uniquely named, this method will resolve any date math used in
     * the provided name, as well as appending a unique identifier so expressions that may overlap
     * still result in unique snapshot names.
     */
    public String generateSnapshotName(Context context) {
        List<String> candidates = DATE_MATH_RESOLVER.resolve(context, Collections.singletonList(this.name));
        if (candidates.size() != 1) {
            throw new IllegalStateException("resolving snapshot name " + this.name + " generated more than one candidate: " + candidates);
        }
        // TODO: we are breaking the rules of UUIDs by lowercasing this here, find an alternative (snapshot names must be lowercase)
        return candidates.get(0) + "-" + UUIDs.randomBase64UUID().toLowerCase(Locale.ROOT);
    }

    /**
     * Generate a new create snapshot request from this policy. The name of the snapshot is
     * generated at this time based on any date math expressions in the "name" field.
     */
    public CreateSnapshotRequest toRequest() {
        CreateSnapshotRequest req = new CreateSnapshotRequest(repository, generateSnapshotName(new ResolverContext()));
        req.source(configuration);
        req.waitForCompletion(false);
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
        out.writeMap(this.configuration);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), this.name);
        builder.field(SCHEDULE.getPreferredName(), this.schedule);
        builder.field(REPOSITORY.getPreferredName(), this.repository);
        builder.field(CONFIG.getPreferredName(), this.configuration);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, schedule, repository, configuration);
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
        return Objects.equals(id, other.id) &&
            Objects.equals(name, other.name) &&
            Objects.equals(schedule, other.schedule) &&
            Objects.equals(repository, other.repository) &&
            Objects.equals(configuration, other.configuration);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * This is a context for the DateMathExpressionResolver, which does not require
     * {@code IndicesOptions} or {@code ClusterState} since it only uses the start
     * time to resolve expressions
     */
    public static final class ResolverContext extends Context {
        public ResolverContext() {
            this(System.currentTimeMillis());
        }

        public ResolverContext(long startTime) {
            super(null, null, startTime, false, false);
        }

        @Override
        public ClusterState getState() {
            throw new UnsupportedOperationException("should never be called");
        }

        @Override
        public IndicesOptions getOptions() {
            throw new UnsupportedOperationException("should never be called");
        }
    }
}
