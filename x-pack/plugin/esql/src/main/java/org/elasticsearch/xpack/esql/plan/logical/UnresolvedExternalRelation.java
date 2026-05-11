/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.singletonList;

/**
 * Unresolved external data source reference (Iceberg table or Parquet file). Produced by the parser
 * for inline {@code EXTERNAL} commands and by the dataset rewriter for {@code FROM <dataset>}; both
 * paths converge here so the downstream resolver/analyzer treat them uniformly.
 *
 * <p>The {@code config} map holds plain configuration values (no {@link
 * org.elasticsearch.xpack.esql.core.expression.Literal} wrappers); secret values arrive as
 * {@link org.elasticsearch.common.settings.SecureString} on the dataset path.
 *
 * @see UnresolvedRelation index-side counterpart for {@code FROM <index>}; if you traverse one and
 * care about FROM-style leaves, consider whether you need the other too.
 */
public class UnresolvedExternalRelation extends LeafPlan implements Unresolvable {

    private final Expression tablePath;
    private final Map<String, Object> config;
    private final String unresolvedMsg;

    /**
     * Creates an unresolved external relation.
     *
     * @param source the source location in the query
     * @param tablePath the resource path or external table identifier (a {@code Literal} or parameter reference)
     * @param config plain-valued configuration (e.g., credentials, format options) — not wrapped in {@code Literal}
     */
    public UnresolvedExternalRelation(Source source, Expression tablePath, Map<String, Object> config) {
        super(source);
        this.tablePath = tablePath;
        this.config = config;
        this.unresolvedMsg = "Unknown external table or Parquet file [" + extractTablePathValue(tablePath) + "]";
    }

    private static String extractTablePathValue(Expression tablePath) {
        if (tablePath instanceof org.elasticsearch.xpack.esql.core.expression.Literal literal && literal.value() != null) {
            Object value = literal.value();
            if (value instanceof BytesRef) {
                return BytesRefs.toString((BytesRef) value);
            }
            return value.toString();
        }
        return tablePath.sourceText();
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<UnresolvedExternalRelation> info() {
        return NodeInfo.create(this, UnresolvedExternalRelation::new, tablePath, config);
    }

    public Expression tablePath() {
        return tablePath;
    }

    public Map<String, Object> config() {
        return config;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), tablePath, config, unresolvedMsg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnresolvedExternalRelation other = (UnresolvedExternalRelation) obj;
        return Objects.equals(tablePath, other.tablePath)
            && Objects.equals(config, other.config)
            && Objects.equals(unresolvedMsg, other.unresolvedMsg);
    }

    @Override
    public List<Object> nodeProperties() {
        // config omitted intentionally — SecureString.toString() would leak plaintext into EXPLAIN.
        return singletonList(tablePath);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + "EXTERNAL[" + tablePath.sourceText() + "]";
    }
}
