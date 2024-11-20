/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class EsSourceExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EsSourceExec",
        EsSourceExec::new
    );

    private final EsIndex index;
    private final List<Attribute> attributes;
    private final QueryBuilder query;
    private final IndexMode indexMode;

    public EsSourceExec(EsRelation relation) {
        this(relation.source(), relation.index(), relation.output(), null, relation.indexMode());
    }

    public EsSourceExec(Source source, EsIndex index, List<Attribute> attributes, QueryBuilder query, IndexMode indexMode) {
        super(source);
        this.index = index;
        this.attributes = attributes;
        this.query = query;
        this.indexMode = indexMode;
    }

    private EsSourceExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            new EsIndex(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            EsRelation.readIndexMode(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        index().writeTo(out);
        out.writeNamedWriteableCollection(output());
        out.writeOptionalNamedWriteable(query());
        EsRelation.writeIndexMode(out, indexMode());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public EsIndex index() {
        return index;
    }

    public QueryBuilder query() {
        return query;
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EsSourceExec::new, index, attributes, query, indexMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, attributes, query, indexMode);
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
        return Objects.equals(index, other.index)
            && Objects.equals(attributes, other.attributes)
            && Objects.equals(query, other.query)
            && Objects.equals(indexMode, other.indexMode);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + index + "]" + NodeUtils.limitedToString(attributes);
    }
}
