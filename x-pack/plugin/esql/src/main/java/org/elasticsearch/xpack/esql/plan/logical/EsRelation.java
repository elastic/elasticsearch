/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.TransportVersions.ESQL_SKIP_ES_INDEX_SERIALIZATION;

public class EsRelation extends LeafPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "EsRelation",
        EsRelation::readFrom
    );

    private final String indexPattern;
    private final IndexMode indexMode;
    private final Map<String, IndexMode> indexNameWithModes;
    private final List<Attribute> attrs;

    public EsRelation(Source source, EsIndex index, IndexMode indexMode) {
        this(source, index.name(), indexMode, index.indexNameWithModes(), flatten(source, index.mapping()));
    }

    public EsRelation(
        Source source,
        String indexPattern,
        IndexMode indexMode,
        Map<String, IndexMode> indexNameWithModes,
        List<Attribute> attributes
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.indexMode = indexMode;
        this.indexNameWithModes = indexNameWithModes;
        this.attrs = attributes;
    }

    private static EsRelation readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        String indexPattern;
        Map<String, IndexMode> indexNameWithModes;
        if (in.getTransportVersion().onOrAfter(ESQL_SKIP_ES_INDEX_SERIALIZATION)) {
            indexPattern = in.readString();
            indexNameWithModes = in.readMap(IndexMode::readFrom);
        } else {
            var index = EsIndex.readFrom(in);
            indexPattern = index.name();
            indexNameWithModes = index.indexNameWithModes();
        }
        List<Attribute> attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        if (supportingEsSourceOptions(in.getTransportVersion())) {
            // We don't do anything with these strings
            in.readOptionalString();
            in.readOptionalString();
            in.readOptionalString();
        }
        IndexMode indexMode = readIndexMode(in);
        if (in.getTransportVersion().before(ESQL_SKIP_ES_INDEX_SERIALIZATION)) {
            in.readBoolean();
        }
        return new EsRelation(source, indexPattern, indexMode, indexNameWithModes, attributes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        if (out.getTransportVersion().onOrAfter(ESQL_SKIP_ES_INDEX_SERIALIZATION)) {
            out.writeString(indexPattern);
            out.writeMap(indexNameWithModes, (o, v) -> IndexMode.writeTo(v, out));
        } else {
            new EsIndex(indexPattern, Map.of(), indexNameWithModes).writeTo(out);
        }
        out.writeNamedWriteableCollection(attrs);
        if (supportingEsSourceOptions(out.getTransportVersion())) {
            // write (null) string fillers expected by remote
            out.writeOptionalString(null);
            out.writeOptionalString(null);
            out.writeOptionalString(null);
        }
        writeIndexMode(out, indexMode);
        if (out.getTransportVersion().before(ESQL_SKIP_ES_INDEX_SERIALIZATION)) {
            out.writeBoolean(false);
        }
    }

    private static boolean supportingEsSourceOptions(TransportVersion version) {
        return version.between(TransportVersions.V_8_14_0, TransportVersions.V_8_15_0);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<EsRelation> info() {
        return NodeInfo.create(this, EsRelation::new, indexPattern, indexMode, indexNameWithModes, attrs);
    }

    private static List<Attribute> flatten(Source source, Map<String, EsField> mapping) {
        return flatten(source, mapping, null);
    }

    private static List<Attribute> flatten(Source source, Map<String, EsField> mapping, FieldAttribute parent) {
        List<Attribute> list = new ArrayList<>();

        for (Entry<String, EsField> entry : mapping.entrySet()) {
            String name = entry.getKey();
            EsField t = entry.getValue();

            if (t != null) {
                FieldAttribute f = new FieldAttribute(
                    source,
                    parent != null ? parent.name() : null,
                    parent != null ? parent.name() + "." + name : name,
                    t
                );
                list.add(f);
                // object or nested
                if (t.getProperties().isEmpty() == false) {
                    list.addAll(flatten(source, t.getProperties(), f));
                }
            }
        }
        return list;
    }

    public String indexPattern() {
        return indexPattern;
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    public Map<String, IndexMode> indexNameWithModes() {
        return indexNameWithModes;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    public Set<String> concreteIndices() {
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
        return Objects.hash(indexPattern, indexMode, indexNameWithModes, attrs);
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
            && Objects.equals(indexNameWithModes, other.indexNameWithModes)
            && Objects.equals(attrs, other.attrs);
    }

    @Override
    public String nodeString() {
        return nodeName()
            + "["
            + indexPattern
            + "]"
            + (indexMode != IndexMode.STANDARD ? "[" + indexMode.name() + "]" : "")
            + NodeUtils.limitedToString(attrs);
    }

    public static IndexMode readIndexMode(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            return IndexMode.fromString(in.readString());
        } else {
            return IndexMode.STANDARD;
        }
    }

    public static void writeIndexMode(StreamOutput out, IndexMode indexMode) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeString(indexMode.getName());
        } else if (indexMode != IndexMode.STANDARD) {
            throw new IllegalStateException("not ready to support index mode [" + indexMode + "]");
        }
    }

    public EsRelation withAttributes(List<Attribute> newAttributes) {
        return new EsRelation(source(), indexPattern, indexMode, indexNameWithModes, newAttributes);
    }
}
