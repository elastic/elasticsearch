/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SnapshotLifecyclePolicy implements ToXContentObject {

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

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SnapshotLifecyclePolicy, String> PARSER =
        new ConstructingObjectParser<>("snapshot_lifecycle", true,
            (a, id) -> {
                String name = (String) a[0];
                String schedule = (String) a[1];
                String repo = (String) a[2];
                Map<String, Object> config = (Map<String, Object>) a[3];
                SnapshotRetentionConfiguration retention = (SnapshotRetentionConfiguration) a[4];
                return new SnapshotLifecyclePolicy(id, name, schedule, repo, config, retention);
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SCHEDULE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), CONFIG);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotRetentionConfiguration::parse, RETENTION);
    }

    public SnapshotLifecyclePolicy(final String id, final String name, final String schedule,
                                   final String repository, @Nullable final Map<String, Object> configuration,
                                   @Nullable final SnapshotRetentionConfiguration retentionPolicy) {
        this.id = Objects.requireNonNull(id, "policy id is required");
        this.name = Objects.requireNonNull(name, "policy snapshot name is required");
        this.schedule = Objects.requireNonNull(schedule, "policy schedule is required");
        this.repository = Objects.requireNonNull(repository, "policy snapshot repository is required");
        this.configuration = configuration;
        this.retentionPolicy = retentionPolicy;
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

    public static SnapshotLifecyclePolicy parse(XContentParser parser, String id) {
        return PARSER.apply(parser, id);
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
        return Objects.equals(id, other.id) &&
            Objects.equals(name, other.name) &&
            Objects.equals(schedule, other.schedule) &&
            Objects.equals(repository, other.repository) &&
            Objects.equals(configuration, other.configuration) &&
            Objects.equals(retentionPolicy, other.retentionPolicy);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
