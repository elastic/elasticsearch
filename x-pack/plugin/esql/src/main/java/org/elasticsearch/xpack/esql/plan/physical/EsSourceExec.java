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
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.ESQ_SKIP_ES_INDEX_SERIALIZATION;

public class EsSourceExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EsSourceExec",
        EsSourceExec::readFrom
    );

    private final String indexName;
    private final IndexMode indexMode;
    private final Map<String, IndexMode> indexNameWithModes;
    private final QueryBuilder query;
    private final List<Attribute> attributes;

    public EsSourceExec(EsRelation relation) {
        this(relation.source(), relation.index().name(), relation.indexMode(), relation.index().indexNameWithModes(), null, relation.output());
    }

    public EsSourceExec(Source source, String indexName, IndexMode indexMode, Map<String, IndexMode> indexNameWithModes, QueryBuilder query, List<Attribute> attributes) {
        super(source);
        this.indexName = indexName;
        this.indexMode = indexMode;
        this.indexNameWithModes = indexNameWithModes;
        this.query = query;
        this.attributes = attributes;
    }

    private static EsSourceExec readFrom(StreamInput in) throws IOException {
        var source = Source.readFrom((PlanStreamInput) in);
        String indexName;
        Map<String, IndexMode> indexNameWithModes;
        if (in.getTransportVersion().onOrAfter(ESQ_SKIP_ES_INDEX_SERIALIZATION)) {
            indexName = in.readString();
            indexNameWithModes = in.readMap(IndexMode::readFrom);
        } else {
            var index = new EsIndex(in);
            indexName = index.name();
            indexNameWithModes = index.indexNameWithModes();
        }
        var attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        var query = in.readOptionalNamedWriteable(QueryBuilder.class);
        var indexMode = EsRelation.readIndexMode(in);
        return new EsSourceExec(source, indexName, indexMode, indexNameWithModes, query, attributes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        if (out.getTransportVersion().onOrAfter(ESQ_SKIP_ES_INDEX_SERIALIZATION)) {
            out.writeString(indexName);
            out.writeMap(indexNameWithModes, (o, v) -> IndexMode.writeTo(v, out));
        } else {
            new EsIndex(indexName, Map.of(), indexNameWithModes).writeTo(out);
        }
        out.writeNamedWriteableCollection(output());
        out.writeOptionalNamedWriteable(query());
        EsRelation.writeIndexMode(out, indexMode());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String indexName() {
        return indexName;
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    public Map<String, IndexMode> indexNameWithModes() {
        return indexNameWithModes;
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
        return NodeInfo.create(this, EsSourceExec::new, indexName, indexMode, indexNameWithModes, query, attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, indexMode, indexNameWithModes, query, attributes);
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
        return Objects.equals(indexName, other.indexName)
            && Objects.equals(indexMode, other.indexMode)
            && Objects.equals(indexNameWithModes, other.indexNameWithModes)
            && Objects.equals(query, other.query)
            && Objects.equals(attributes, other.attributes);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + indexName + "]" + NodeUtils.limitedToString(attributes);
    }
}
