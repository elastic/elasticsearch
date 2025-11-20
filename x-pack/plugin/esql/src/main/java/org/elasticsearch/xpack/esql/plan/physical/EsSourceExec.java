/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class EsSourceExec extends LeafExec {

    protected static final TransportVersion REMOVE_NAME_WITH_MODS = TransportVersion.fromName("esql_es_source_remove_name_with_mods");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EsSourceExec",
        EsSourceExec::readFrom
    );

    private final String indexPattern;
    private final IndexMode indexMode;
    private final List<Attribute> attributes;
    private final QueryBuilder query;

    public EsSourceExec(EsRelation relation) {
        this(relation.source(), relation.indexPattern(), relation.indexMode(), relation.output(), null);
    }

    public EsSourceExec(Source source, String indexPattern, IndexMode indexMode, List<Attribute> attributes, QueryBuilder query) {
        super(source);
        this.indexPattern = indexPattern;
        this.indexMode = indexMode;
        this.attributes = attributes;
        this.query = query;
    }

    private static EsSourceExec readFrom(StreamInput in) throws IOException {
        var source = Source.readFrom((PlanStreamInput) in);
        String indexPattern = in.readString();
        if (in.getTransportVersion().supports(REMOVE_NAME_WITH_MODS) == false) {
            in.readMap(IndexMode::readFrom);
        }
        if (in.getTransportVersion().supports(TransportVersions.V_8_18_0) == false) {
            in.readImmutableMap(StreamInput::readString, EsField::readFrom);
        }
        var attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        var query = in.readOptionalNamedWriteable(QueryBuilder.class);
        var indexMode = IndexMode.fromString(in.readString());
        return new EsSourceExec(source, indexPattern, indexMode, attributes, query);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(indexPattern);
        if (out.getTransportVersion().supports(TransportVersions.V_8_18_0) == false) {
            out.writeMap(Map.<String, EsField>of(), (o, x) -> x.writeTo(out));
        }
        if (out.getTransportVersion().supports(REMOVE_NAME_WITH_MODS) == false) {
            out.writeMap(Map.<String, IndexMode>of(), (o, v) -> IndexMode.writeTo(v, out));
        }
        out.writeNamedWriteableCollection(output());
        out.writeOptionalNamedWriteable(query());
        out.writeString(indexMode().getName());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String indexPattern() {
        return indexPattern;
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    public QueryBuilder query() {
        return query;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EsSourceExec::new, indexPattern, indexMode, attributes, query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern, indexMode, attributes, query);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsSourceExec other = (EsSourceExec) obj;
        return Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(indexMode, other.indexMode)
            && Objects.equals(attributes, other.attributes)
            && Objects.equals(query, other.query);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + indexPattern + "]" + NodeUtils.limitedToString(attributes);
    }
}
