/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.process.ModelSnapshot;
import org.elasticsearch.common.xcontent.ParseField;
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
