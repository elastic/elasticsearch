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

package org.elasticsearch.client.snapshotlifecycle;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class SnapshotLifecyclePolicyMetadata implements ToXContentObject {

    static final ParseField POLICY = new ParseField("policy");
    static final ParseField VERSION = new ParseField("version");
    static final ParseField MODIFIED_DATE_MILLIS = new ParseField("modified_date_millis");
    static final ParseField MODIFIED_DATE = new ParseField("modified_date");
    static final ParseField LAST_SUCCESS = new ParseField("last_success");
    static final ParseField LAST_FAILURE = new ParseField("last_failure");
    static final ParseField NEXT_EXECUTION_MILLIS = new ParseField("next_execution_millis");
    static final ParseField NEXT_EXECUTION = new ParseField("next_execution");

    private final SnapshotLifecyclePolicy policy;
    private final long version;
    private final long modifiedDate;
    private final long nextExecution;
    @Nullable
    private final SnapshotInvocationRecord lastSuccess;
    @Nullable
    private final SnapshotInvocationRecord lastFailure;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SnapshotLifecyclePolicyMetadata, String> PARSER =
        new ConstructingObjectParser<>("snapshot_policy_metadata",
            a -> {
                SnapshotLifecyclePolicy policy = (SnapshotLifecyclePolicy) a[0];
                long version = (long) a[1];
                long modifiedDate = (long) a[2];
                SnapshotInvocationRecord lastSuccess = (SnapshotInvocationRecord) a[3];
                SnapshotInvocationRecord lastFailure = (SnapshotInvocationRecord) a[4];
                long nextExecution = (long) a[5];

                return new SnapshotLifecyclePolicyMetadata(policy, version, modifiedDate, lastSuccess, lastFailure, nextExecution);
            });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), SnapshotLifecyclePolicy::parse, POLICY);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODIFIED_DATE_MILLIS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotInvocationRecord::parse, LAST_SUCCESS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotInvocationRecord::parse, LAST_FAILURE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), NEXT_EXECUTION_MILLIS);
    }

    public static SnapshotLifecyclePolicyMetadata parse(XContentParser parser, String id) {
        return PARSER.apply(parser, id);
    }

    public SnapshotLifecyclePolicyMetadata(SnapshotLifecyclePolicy policy, long version, long modifiedDate,
                                           SnapshotInvocationRecord lastSuccess, SnapshotInvocationRecord lastFailure,
                                           long nextExecution) {
        this.policy = policy;
        this.version = version;
        this.modifiedDate = modifiedDate;
        this.lastSuccess = lastSuccess;
        this.lastFailure = lastFailure;
        this.nextExecution = nextExecution;
    }

    public SnapshotLifecyclePolicy getPolicy() {
        return policy;
    }

    public String getName() {
        return policy.getName();
    }

    public long getVersion() {
        return version;
    }

    public long getModifiedDate() {
        return modifiedDate;
    }

    public SnapshotInvocationRecord getLastSuccess() {
        return lastSuccess;
    }

    public SnapshotInvocationRecord getLastFailure() {
        return lastFailure;
    }

    public long getNextExecution() {
        return this.nextExecution;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY.getPreferredName(), policy);
        builder.field(VERSION.getPreferredName(), version);
        builder.timeField(MODIFIED_DATE_MILLIS.getPreferredName(), MODIFIED_DATE.getPreferredName(), modifiedDate);
        if (Objects.nonNull(lastSuccess)) {
            builder.field(LAST_SUCCESS.getPreferredName(), lastSuccess);
        }
        if (Objects.nonNull(lastFailure)) {
            builder.field(LAST_FAILURE.getPreferredName(), lastFailure);
        }
        builder.timeField(NEXT_EXECUTION_MILLIS.getPreferredName(), NEXT_EXECUTION.getPreferredName(), nextExecution);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, version, modifiedDate, lastSuccess, lastFailure, nextExecution);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SnapshotLifecyclePolicyMetadata other = (SnapshotLifecyclePolicyMetadata) obj;
        return Objects.equals(policy, other.policy) &&
            Objects.equals(version, other.version) &&
            Objects.equals(modifiedDate, other.modifiedDate) &&
            Objects.equals(lastSuccess, other.lastSuccess) &&
            Objects.equals(lastFailure, other.lastFailure) &&
            Objects.equals(nextExecution, other.nextExecution);
    }

}
