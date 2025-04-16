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

import static org.elasticsearch.TransportVersions.ESQL_SKIP_ES_INDEX_SERIALIZATION;

public class EsSourceExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EsSourceExec",
        EsSourceExec::readFrom
    );

    private final String indexPattern;
    private final IndexMode indexMode;
    private final Map<String, IndexMode> indexNameWithModes;
    private final List<Attribute> attributes;
    private final QueryBuilder query;

    public EsSourceExec(EsRelation relation) {
        this(relation.source(), relation.indexPattern(), relation.indexMode(), relation.indexNameWithModes(), relation.output(), null);
    }

    public EsSourceExec(
        Source source,
        String indexPattern,
        IndexMode indexMode,
        Map<String, IndexMode> indexNameWithModes,
        List<Attribute> attributes,
        QueryBuilder query
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.indexMode = indexMode;
        this.indexNameWithModes = indexNameWithModes;
        this.attributes = attributes;
        this.query = query;
    }

    private static EsSourceExec readFrom(StreamInput in) throws IOException {
        var source = Source.readFrom((PlanStreamInput) in);
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
        var attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        var query = in.readOptionalNamedWriteable(QueryBuilder.class);
        var indexMode = EsRelation.readIndexMode(in);
        return new EsSourceExec(source, indexPattern, indexMode, indexNameWithModes, attributes, query);
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
        out.writeNamedWriteableCollection(output());
        out.writeOptionalNamedWriteable(query());
        EsRelation.writeIndexMode(out, indexMode());
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
        return NodeInfo.create(this, EsSourceExec::new, indexPattern, indexMode, indexNameWithModes, attributes, query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern, indexMode, indexNameWithModes, attributes, query);
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
            && Objects.equals(indexNameWithModes, other.indexNameWithModes)
            && Objects.equals(attributes, other.attributes)
            && Objects.equals(query, other.query);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + indexPattern + "]" + NodeUtils.limitedToString(attributes);
    }
}
