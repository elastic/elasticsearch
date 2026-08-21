/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Coordinator-local physical leaf for the {@code EQL "<query>"} source command. At execution time the
 * source operator issues an {@code EqlSearchAction} against the target indices and converts the bounded
 * response into a single page under the fixed schema (see {@link EqlRelation}). It is never wrapped in a
 * {@code FragmentExec}, so it stays on the coordinating node and is not shipped to data nodes.
 */
public class EqlSourceExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EqlSourceExec",
        EqlSourceExec::new
    );

    private final String query;
    private final String indices;
    private final Map<String, Object> options;
    private final EqlRelation.Mode mode;
    private final List<Attribute> attributes;
    private final Integer pushedLimit;
    // The merged field-caps ES|QL already resolved for this pattern; the planner injects it into the EQL request so the
    // EQL engine skips its own resolution, or null when none was retained. Never serialized (a deserialized copy is null
    // and simply falls back to the engine re-resolving); excluded from equals/hashCode (a redundant resolution, not plan
    // semantics).
    @Nullable
    private final FieldCapabilitiesResponse preResolvedFieldCaps;

    public EqlSourceExec(
        Source source,
        String query,
        String indices,
        Map<String, Object> options,
        EqlRelation.Mode mode,
        List<Attribute> attributes,
        Integer pushedLimit,
        @Nullable FieldCapabilitiesResponse preResolvedFieldCaps
    ) {
        super(source);
        this.query = query;
        this.indices = indices;
        this.options = options;
        this.mode = mode;
        this.attributes = attributes;
        this.pushedLimit = pushedLimit;
        this.preResolvedFieldCaps = preResolvedFieldCaps;
    }

    @SuppressWarnings("unchecked")
    private EqlSourceExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readString(),
            in.readString(),
            (Map<String, Object>) in.readGenericValue(),
            in.readEnum(EqlRelation.Mode.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readOptionalVInt(),
            null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Physical plans do not preserve Source across the wire (it is coordinator-only); mirror EsSourceExec.
        Source.EMPTY.writeTo(out);
        out.writeString(query);
        out.writeString(indices);
        out.writeGenericValue(options);
        out.writeEnum(mode);
        out.writeNamedWriteableCollection(attributes);
        out.writeOptionalVInt(pushedLimit);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EqlSourceExec::new, query, indices, options, mode, attributes, pushedLimit, preResolvedFieldCaps);
    }

    public String query() {
        return query;
    }

    public String indices() {
        return indices;
    }

    /** The row {@code LIMIT} folded into the request size, or {@code null} to fall back to the truncation cap. */
    public Integer pushedLimit() {
        return pushedLimit;
    }

    /** The coordinator-resolved field-caps to reuse for the EQL delegate, or {@code null} if none was retained. */
    @Nullable
    public FieldCapabilitiesResponse preResolvedFieldCaps() {
        return preResolvedFieldCaps;
    }

    public Map<String, Object> options() {
        return options;
    }

    public EqlRelation.Mode mode() {
        return mode;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, indices, options, mode, attributes, pushedLimit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EqlSourceExec other = (EqlSourceExec) obj;
        return Objects.equals(query, other.query)
            && Objects.equals(indices, other.indices)
            && Objects.equals(options, other.options)
            && mode == other.mode
            && Objects.equals(attributes, other.attributes)
            && Objects.equals(pushedLimit, other.pushedLimit);
    }
}
