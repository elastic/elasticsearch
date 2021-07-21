/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ExecuteSnapshotLifecyclePolicyResponse implements ToXContentObject {

    private static final ParseField SNAPSHOT_NAME = new ParseField("snapshot_name");
    private static final ConstructingObjectParser<ExecuteSnapshotLifecyclePolicyResponse, Void> PARSER =
        new ConstructingObjectParser<>("excecute_snapshot_policy", true,
            a -> new ExecuteSnapshotLifecyclePolicyResponse((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_NAME);
    }

    private final String snapshotName;

    public ExecuteSnapshotLifecyclePolicyResponse(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public static ExecuteSnapshotLifecyclePolicyResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public String getSnapshotName() {
        return this.snapshotName;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SNAPSHOT_NAME.getPreferredName(), snapshotName);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExecuteSnapshotLifecyclePolicyResponse other = (ExecuteSnapshotLifecyclePolicyResponse) o;
        return this.snapshotName.equals(other.snapshotName);
    }

    @Override
    public int hashCode() {
        return this.snapshotName.hashCode();
    }
}
