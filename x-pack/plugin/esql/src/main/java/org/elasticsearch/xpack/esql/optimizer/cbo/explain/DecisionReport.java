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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Provenance;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * One optimizer decision point, such as a join strategy or an aggregation's distribution, explained for a reader of the profile.
 * The verdict comes first ({@link #summary()}, {@link #mode()}, {@link #chosen()}, {@link #outcome()}), followed by the
 * evidence: the statistics it relied on, the alternatives it beat, and the alternatives it never considered.
 */
public record DecisionReport(
    String id,
    String kind,
    String node,
    @Nullable SourceAnchor source,
    String summary,
    DecisionMode mode,
    ChosenReport chosen,
    @Nullable Outcome outcome,
    String required,
    List<String> anchor,
    Provenance estimateBasis,
    List<NamedEstimate> inputs,
    List<AlternativeReport> alternatives,
    List<Exclusion> excluded,
    boolean searchAnomaly,
    @Nullable String costWarning
) implements Writeable, ToXContentObject {

    public DecisionReport {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(summary, "summary");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(chosen, "chosen");
        Objects.requireNonNull(required, "required");
        Objects.requireNonNull(estimateBasis, "estimateBasis");
        anchor = anchor == null ? List.of() : List.copyOf(anchor);
        inputs = inputs == null ? List.of() : List.copyOf(inputs);
        alternatives = alternatives == null ? List.of() : List.copyOf(alternatives);
        excluded = excluded == null ? List.of() : List.copyOf(excluded);
    }

    public DecisionReport(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readString(),
            in.readString(),
            in.readOptionalWriteable(SourceAnchor::new),
            in.readString(),
            in.readEnum(DecisionMode.class),
            new ChosenReport(in),
            in.readOptionalWriteable(Outcome::new),
            in.readString(),
            in.readStringCollectionAsImmutableList(),
            in.readEnum(Provenance.class),
            in.readCollectionAsImmutableList(NamedEstimate::new),
            in.readCollectionAsImmutableList(AlternativeReport::new),
            in.readCollectionAsImmutableList(Exclusion::new),
            in.readBoolean(),
            in.readOptionalString()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(kind);
        out.writeString(node);
        out.writeOptionalWriteable(source);
        out.writeString(summary);
        out.writeEnum(mode);
        chosen.writeTo(out);
        out.writeOptionalWriteable(outcome);
        out.writeString(required);
        out.writeStringCollection(anchor);
        out.writeEnum(estimateBasis);
        out.writeCollection(inputs);
        out.writeCollection(alternatives);
        out.writeCollection(excluded);
        out.writeBoolean(searchAnomaly);
        out.writeOptionalString(costWarning);
    }

    /** Returns a copy with the outcome observed in the final physical plan. */
    public DecisionReport withOutcome(@Nullable Outcome o) {
        return new DecisionReport(
            id,
            kind,
            node,
            source,
            summary,
            mode,
            chosen,
            o,
            required,
            anchor,
            estimateBasis,
            inputs,
            alternatives,
            excluded,
            searchAnomaly,
            costWarning
        );
    }

    /** Returns a copy with a different summary sentence, for example one that mentions the outcome. */
    public DecisionReport withSummary(String s) {
        return new DecisionReport(
            id,
            kind,
            node,
            source,
            s,
            mode,
            chosen,
            outcome,
            required,
            anchor,
            estimateBasis,
            inputs,
            alternatives,
            excluded,
            searchAnomaly,
            costWarning
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("id", id);
        b.field("kind", kind);
        b.field("node", node);
        ExplainXContent.optionalObject(b, "source", source, params);
        b.field("summary", summary);
        b.field("mode", mode.displayName());
        b.field("chosen");
        chosen.toXContent(b, params);
        ExplainXContent.optionalObject(b, "outcome", outcome, params);
        b.field("required", required);
        if (anchor.isEmpty() == false) {
            b.stringListField("anchor", anchor);
        }
        b.field("estimate_basis", ExplainXContent.lowercase(estimateBasis));
        if (inputs.isEmpty() == false) {
            b.startObject("inputs");
            for (NamedEstimate input : inputs) {
                b.field(input.name());
                input.estimate().toXContent(b, params);
            }
            b.endObject();
        }
        ExplainXContent.optionalArray(b, "alternatives", alternatives, params);
        ExplainXContent.optionalArray(b, "excluded", excluded, params);
        if (searchAnomaly) {
            b.field("search_anomaly", true);
        }
        ExplainXContent.optionalField(b, "cost_warning", costWarning);
        return b.endObject();
    }
}
