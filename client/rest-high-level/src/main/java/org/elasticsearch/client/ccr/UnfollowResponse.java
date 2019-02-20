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

package org.elasticsearch.client.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public final class UnfollowResponse {

    static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");
    static final ParseField RETENTION_LEASES_REMOVED = new ParseField("retention_leases_removed");
    static final ParseField RETENTION_LEASES_REMOVAL_FAILURE_CAUSE = new ParseField("retention_leases_removal_failure_cause");

    private static final ConstructingObjectParser<UnfollowResponse, Void> PARSER = new ConstructingObjectParser<>(
            "unfollow_response", true, args -> new UnfollowResponse((boolean) args[0], (boolean) args[1], (ElasticsearchException) args[2]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ACKNOWLEDGED);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), RETENTION_LEASES_REMOVED);
        PARSER.declareObject(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> ElasticsearchException.fromXContent(p),
                RETENTION_LEASES_REMOVAL_FAILURE_CAUSE);
    }

    public static UnfollowResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final boolean acknowledged;

    public boolean isAcknowledged() {
        return acknowledged;
    }

    private final boolean retentionLeasesRemoved;

    public boolean isRetentionLeasesRemoved() {
        return retentionLeasesRemoved;
    }

    private final ElasticsearchException retentionLeasesRemovalFailureCause;

    public ElasticsearchException retentionLeasesRemovalFailureCause() {
        return retentionLeasesRemovalFailureCause;
    }

    UnfollowResponse(
            final boolean acknowledged,
            final boolean retentionLeasesRemoved,
            final ElasticsearchException retentionLeasesRemovalFailureCause) {
        this.acknowledged = acknowledged;
        this.retentionLeasesRemoved = retentionLeasesRemoved;
        this.retentionLeasesRemovalFailureCause = retentionLeasesRemovalFailureCause;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnfollowResponse that = (UnfollowResponse) o;
        return acknowledged == that.acknowledged &&
                retentionLeasesRemoved == that.retentionLeasesRemoved &&
                Objects.equals(retentionLeasesRemovalFailureCause, that.retentionLeasesRemovalFailureCause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledged, retentionLeasesRemoved, retentionLeasesRemovalFailureCause);
    }

}
