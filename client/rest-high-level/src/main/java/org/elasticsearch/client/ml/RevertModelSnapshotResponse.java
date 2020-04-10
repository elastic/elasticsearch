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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.process.ModelSnapshot;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A response containing the reverted model snapshot
 */
public class RevertModelSnapshotResponse implements ToXContentObject {

    private static final ParseField MODEL = new ParseField("model");

    public static final ConstructingObjectParser<RevertModelSnapshotResponse, Void> PARSER =
            new ConstructingObjectParser<>("revert_model_snapshot_response", true,
                a -> new RevertModelSnapshotResponse((ModelSnapshot.Builder) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), ModelSnapshot.PARSER, MODEL);
    }

    public static RevertModelSnapshotResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public RevertModelSnapshotResponse(ModelSnapshot.Builder modelSnapshot) {
        this.model = modelSnapshot.build();
    }

    private final ModelSnapshot model;

    /**
     * Get full information about the reverted model snapshot
     * @return the reverted model snapshot.
     */
    public  ModelSnapshot getModel() {
        return model;
    }

    @Override
    public int hashCode() {
        return Objects.hash(model);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RevertModelSnapshotResponse other = (RevertModelSnapshotResponse) obj;
        return Objects.equals(model, other.model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (model != null) {
            builder.field(MODEL.getPreferredName(), model);
        }
        builder.endObject();
        return builder;
    }
}
