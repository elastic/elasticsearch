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

import org.elasticsearch.common.ParseField;
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
