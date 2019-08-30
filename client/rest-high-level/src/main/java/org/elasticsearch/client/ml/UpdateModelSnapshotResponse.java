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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A response acknowledging the update of information for an existing model snapshot for a given job
 */
public class UpdateModelSnapshotResponse implements ToXContentObject {

    private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");
    private static final ParseField MODEL = new ParseField("model");

    public UpdateModelSnapshotResponse(boolean acknowledged, ModelSnapshot.Builder modelSnapshot) {
        this.acknowledged = acknowledged;
        this.model = modelSnapshot.build();
    }

    public static final ConstructingObjectParser<UpdateModelSnapshotResponse, Void> PARSER =
        new ConstructingObjectParser<>("update_model_snapshot_response", true,
            a -> new UpdateModelSnapshotResponse((Boolean) a[0], ((ModelSnapshot.Builder) a[1])));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ACKNOWLEDGED);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), ModelSnapshot.PARSER, MODEL);
    }

    public static UpdateModelSnapshotResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Boolean acknowledged;
    private final ModelSnapshot model;

    /**
     * Get the action acknowledgement
     * @return  a {@code boolean} that indicates whether the model snapshot was updated successfully.
     */
    public Boolean getAcknowledged() {
        return acknowledged;
    }

    /**
     * Get the updated snapshot of the model
     * @return the updated model snapshot.
     */
    public  ModelSnapshot getModel() {
        return model;
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledged, model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (acknowledged != null) {
            builder.field(ACKNOWLEDGED.getPreferredName(), acknowledged);
        }
        if (model != null) {
            builder.field(MODEL.getPreferredName(), model);
        }
        builder.endObject();
        return builder;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UpdateModelSnapshotResponse request = (UpdateModelSnapshotResponse) obj;
        return Objects.equals(acknowledged, request.acknowledged)
            && Objects.equals(model, request.model);
    }
}
