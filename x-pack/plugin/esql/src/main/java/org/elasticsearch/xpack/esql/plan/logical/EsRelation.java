/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Source representing an Elasticsearch index.
 * <p>
 * In {@code FROM idx | ...} this corresponds to the {@code FROM}.
 */
public class EsRelation extends LeafPlan {

    private static final TransportVersion SPLIT_INDICES = TransportVersion.fromName("esql_es_relation_add_split_indices");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "EsRelation",
        EsRelation::readFrom
    );

    private final String indexPattern;
    private final IndexMode indexMode;
    private final Map<String, List<String>> originalIndices; // keyed by cluster alias
    private final Map<String, List<String>> concreteIndices; // keyed by cluster alias
    private final Map<String, IndexMode> indexNameWithModes;
    private final List<Attribute> attrs;

    public EsRelation(
        Source source,
        String indexPattern,
        IndexMode indexMode,
        Map<String, List<String>> originalIndices,
        Map<String, List<String>> concreteIndices,
        Map<String, IndexMode> indexNameWithModes,
        List<Attribute> attributes
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.indexMode = indexMode;
        this.originalIndices = originalIndices;
        this.concreteIndices = concreteIndices;
        this.indexNameWithModes = indexNameWithModes;
        this.attrs = attributes;
    }

    private static EsRelation readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        String indexPattern = in.readString();
        Map<String, List<String>> originalIndices;
        Map<String, List<String>> concreteIndices;
        if (in.getTransportVersion().supports(SPLIT_INDICES)) {
            originalIndices = in.readMapOfLists(StreamInput::readString);
            concreteIndices = in.readMapOfLists(StreamInput::readString);
        } else {
            originalIndices = Map.of();
            concreteIndices = Map.of();
        }
        Map<String, IndexMode> indexNameWithModes = in.readMap(IndexMode::readFrom);
        List<Attribute> attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        IndexMode indexMode = IndexMode.fromString(in.readString());
        return new EsRelation(source, indexPattern, indexMode, originalIndices, concreteIndices, indexNameWithModes, attributes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(indexPattern);
        if (out.getTransportVersion().supports(SPLIT_INDICES)) {
            out.writeMap(originalIndices, StreamOutput::writeStringCollection);
            out.writeMap(concreteIndices, StreamOutput::writeStringCollection);
        }
        out.writeMap(indexNameWithModes, (o, v) -> IndexMode.writeTo(v, out));
        out.writeNamedWriteableCollection(attrs);
        out.writeString(indexMode.getName());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<EsRelation> info() {
        return NodeInfo.create(this, EsRelation::new, indexPattern, indexMode, originalIndices, concreteIndices, indexNameWithModes, attrs);
    }

    public String indexPattern() {
        return indexPattern;
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    public Map<String, List<String>> originalIndices() {
        return originalIndices;
    }

    public Map<String, List<String>> concreteIndices() {
        return concreteIndices;
    }

    public Map<String, IndexMode> indexNameWithModes() {
        return indexNameWithModes;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    public Set<String> concreteQualifiedIndices() {
        return indexNameWithModes.keySet();
    }

    @Override
    public boolean expressionsResolved() {
        // For unresolved expressions to exist in EsRelation is fine, as long as they are not used in later operations
        // This allows for them to be converted to null@unsupported fields in final output, an important feature of ES|QL
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern, indexMode, originalIndices, concreteIndices, indexNameWithModes, attrs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsRelation other = (EsRelation) obj;
        return Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(indexMode, other.indexMode)
            && Objects.equals(originalIndices, other.originalIndices)
            && Objects.equals(concreteIndices, other.concreteIndices)
            && Objects.equals(indexNameWithModes, other.indexNameWithModes)
            && Objects.equals(attrs, other.attrs);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName()).append('[').append(mapper.index(indexPattern)).append(']');
        if (indexMode != IndexMode.STANDARD) {
            sb.append('[').append(indexMode.name()).append(']');
        }
        // The concrete indices a pattern resolved to, each routed through the index mapper. Rendered
        // in both modes — useful when debugging from a failure log. NOTE: this is NEW under the
        // identity mapper (previously anon-only), so for the cases below EXPLAIN gains a suffix it did
        // not have before. Shown only when resolution actually changed the pattern (see
        // resolvedIndicesAddInfo) — an explicit "FROM a,b" that resolves to exactly itself adds only
        // redundant noise, so it is suppressed.
        if (resolvedIndicesAddInfo()) {
            sb.append('[');
            boolean first = true;
            for (var e : indexNameWithModes.entrySet()) {
                if (first == false) {
                    sb.append(", ");
                }
                first = false;
                sb.append(mapper.index(e.getKey())).append('=').append(e.getValue().name());
            }
            sb.append(']');
        }
        NodeUtils.toString(sb, attrs, format, mapper);
    }

    /**
     * Whether the resolved concrete indices add information beyond the index pattern itself — i.e.
     * resolution expanded or changed the index NAMES (a wildcard / alias resolving to different
     * concrete indices). An explicit {@code FROM a,b} that resolves to exactly {@code {a, b}} echoes
     * the pattern and is suppressed; the relation's {@code indexMode} is already rendered separately,
     * so a mode difference alone (with the names unchanged) is not on its own informative here.
     */
    private boolean resolvedIndicesAddInfo() {
        if (indexNameWithModes == null || indexNameWithModes.isEmpty()) {
            return false;
        }
        Set<String> patternParts = new HashSet<>();
        for (String part : indexPattern.split(",")) {
            patternParts.add(part.trim());
        }
        return patternParts.equals(indexNameWithModes.keySet()) == false;
    }

    public EsRelation withAttributes(List<Attribute> newAttributes) {
        return new EsRelation(source(), indexPattern, indexMode, originalIndices, concreteIndices, indexNameWithModes, newAttributes);
    }

    public EsRelation withAdditionalAttributes(List<? extends Attribute> additionalAttributes) {
        if (additionalAttributes.isEmpty()) {
            return this;
        }
        List<Attribute> newAttrs = new ArrayList<>(attrs.size() + additionalAttributes.size());
        newAttrs.addAll(attrs);
        newAttrs.addAll(additionalAttributes);
        return withAttributes(newAttrs);
    }

    public EsRelation withAdditionalAttribute(Attribute additionalAttribute) {
        return withAdditionalAttributes(List.of(additionalAttribute));
    }

    public EsRelation withIndexMode(IndexMode indexMode) {
        return new EsRelation(source(), indexPattern, indexMode, originalIndices, concreteIndices, indexNameWithModes, attrs);
    }
}
