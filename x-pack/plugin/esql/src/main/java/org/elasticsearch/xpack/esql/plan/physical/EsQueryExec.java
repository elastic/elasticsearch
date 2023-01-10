/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Experimental
public class EsQueryExec extends LeafExec {

    static final EsField DOC_ID_FIELD = new EsField("_doc_id", DataTypes.INTEGER, Map.of(), false);
    static final EsField SEGMENT_ID_FIELD = new EsField("_segment_id", DataTypes.INTEGER, Map.of(), false);
    static final EsField SHARD_ID_FIELD = new EsField("_shard_id", DataTypes.INTEGER, Map.of(), false);
    public static final Set<String> NAMES_SET = Set.of("_doc_id", "_segment_id", "_shard_id");

    public static boolean isSourceAttribute(Attribute attr) {
        return NAMES_SET.contains(attr.name());
    }

    private final EsIndex index;
    private final QueryBuilder query;
    private final List<Attribute> attrs;

    public EsQueryExec(Source source, EsIndex index, QueryBuilder query) {
        this(
            source,
            index,
            List.of(
                new FieldAttribute(source, DOC_ID_FIELD.getName(), DOC_ID_FIELD),
                new FieldAttribute(source, SEGMENT_ID_FIELD.getName(), SEGMENT_ID_FIELD),
                new FieldAttribute(source, SHARD_ID_FIELD.getName(), SHARD_ID_FIELD)
            ),
            query
        );
    }

    public EsQueryExec(Source source, EsIndex index, List<Attribute> attrs, QueryBuilder query) {
        super(source);
        this.index = index;
        this.query = query;
        this.attrs = attrs;
    }

    @Override
    protected NodeInfo<EsQueryExec> info() {
        return NodeInfo.create(this, EsQueryExec::new, index, attrs, query);
    }

    public EsIndex index() {
        return index;
    }

    public QueryBuilder query() {
        return query;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, attrs, query);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsQueryExec other = (EsQueryExec) obj;
        return Objects.equals(index, other.index) && Objects.equals(attrs, other.attrs) && Objects.equals(query, other.query);
    }

    @Override
    public boolean singleNode() {
        return false;
    }

    @Override
    public String nodeString() {
        return nodeName()
            + "["
            + index
            + "], query["
            + (query != null ? Strings.toString(query, false, true) : "")
            + "]"
            + NodeUtils.limitedToString(attrs);
    }
}
