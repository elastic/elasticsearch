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
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A response containing the requested snapshots
 */
public class GetModelSnapshotsResponse extends AbstractResultResponse<ModelSnapshot> {

    public static final ParseField SNAPSHOTS = new ParseField("model_snapshots");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetModelSnapshotsResponse, Void> PARSER =
            new ConstructingObjectParser<>("get_model_snapshots_response", true,
                    a -> new GetModelSnapshotsResponse((List<ModelSnapshot.Builder>) a[0], (long) a[1]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), ModelSnapshot.PARSER, SNAPSHOTS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), COUNT);
    }

    public static GetModelSnapshotsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    GetModelSnapshotsResponse(List<ModelSnapshot.Builder> snapshotBuilders, long count) {
        super(SNAPSHOTS, snapshotBuilders.stream().map(ModelSnapshot.Builder::build).collect(Collectors.toList()), count);
    }

    /**
     * The retrieved snapshots
     * @return the retrieved snapshots
     */
    public List<ModelSnapshot> snapshots() {
        return results;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, results);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetModelSnapshotsResponse other = (GetModelSnapshotsResponse) obj;
        return count == other.count && Objects.equals(results, other.results);
    }
}
