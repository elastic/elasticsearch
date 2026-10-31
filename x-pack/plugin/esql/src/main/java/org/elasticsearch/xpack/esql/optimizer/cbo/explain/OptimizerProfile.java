/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * The cost-based optimizer section of an ES|QL query profile, explaining why each planning decision was made.
 * {@link #costModel()} identifies the cost formulas, so a saved profile stays interpretable after the model changes.
 */
public record OptimizerProfile(String costModel, List<RunReport> runs, int truncatedRuns) implements Writeable, ToXContentObject {

    /** Identifier of the cost formulas; change it whenever they change. */
    public static final String COST_MODEL = "esql_cbo_v1";

    /** How {@link CostVector#score()} ranks plans, stated in the report so readers do not need the source. */
    public static final String SCORE_FORMULA = "latency_seconds + 1e-9 * work_units";

    public OptimizerProfile {
        Objects.requireNonNull(costModel, "costModel");
        runs = runs == null ? List.of() : List.copyOf(runs);
    }

    public OptimizerProfile(StreamInput in) throws IOException {
        this(in.readString(), in.readCollectionAsImmutableList(RunReport::new), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(costModel);
        out.writeCollection(runs);
        out.writeVInt(truncatedRuns);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("cost_model", costModel);
        b.field("score_formula", SCORE_FORMULA);
        ExplainXContent.optionalArray(b, "runs", runs, params);
        if (truncatedRuns > 0) {
            b.field("truncated_runs", truncatedRuns);
        }
        return b.endObject();
    }
}
