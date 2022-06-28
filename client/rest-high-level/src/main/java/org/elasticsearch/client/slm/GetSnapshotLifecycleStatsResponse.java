/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class GetSnapshotLifecycleStatsResponse implements ToXContentObject {

    private final SnapshotLifecycleStats stats;

    public GetSnapshotLifecycleStatsResponse(SnapshotLifecycleStats stats) {
        this.stats = stats;
    }

    public SnapshotLifecycleStats getStats() {
        return this.stats;
    }

    public static GetSnapshotLifecycleStatsResponse fromXContent(XContentParser parser) throws IOException {
        return new GetSnapshotLifecycleStatsResponse(SnapshotLifecycleStats.parse(parser));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return stats.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GetSnapshotLifecycleStatsResponse other = (GetSnapshotLifecycleStatsResponse) o;
        return Objects.equals(this.stats, other.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.stats);
    }
}
