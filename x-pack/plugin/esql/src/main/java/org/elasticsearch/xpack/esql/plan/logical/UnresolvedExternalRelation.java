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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
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
    private final List<NamedExpression> metadataFields;
    private final String unresolvedMsg;

    /**
     * Creates an unresolved external relation with no METADATA fields. Convenience overload for
     * callers that don't carry standard metadata names (the inline {@code EXTERNAL} command path,
     * tests).
     */
    public UnresolvedExternalRelation(Source source, Expression tablePath, Map<String, Object> config) {
        this(source, tablePath, config, List.of());
    }

    /**
     * Creates an unresolved external relation.
     * <p>
     * The {@code metadataFields} list carries the names from the user's {@code METADATA ...} clause
     * verbatim. The analyzer (specifically {@code ResolveExternalRelations}) is the binding site:
     * it resolves each name against {@link org.elasticsearch.xpack.esql.core.expression.MetadataAttribute#ATTRIBUTES_MAP}
     * and appends an {@link org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute}
     * per resolved name to the leaf's output. This constructor does not validate the names — invalid
     * names surface as unresolved attributes downstream with the existing "Unknown column" diagnostic.
     *
     * @param source the source location in the query
     * @param tablePath the resource path or external table identifier (a {@code Literal} or parameter reference)
     * @param config plain-valued configuration (e.g., credentials, format options) — not wrapped in {@code Literal}
     * @param metadataFields names requested in the {@code METADATA} clause, in declaration order; never {@code null}
     */
    public UnresolvedExternalRelation(
        Source source,
        Expression tablePath,
        Map<String, Object> config,
        List<NamedExpression> metadataFields
    ) {
        super(source);
        this.tablePath = tablePath;
        this.config = config;
        this.metadataFields = Objects.requireNonNull(metadataFields, "metadataFields");
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
        return NodeInfo.create(this, UnresolvedExternalRelation::new, tablePath, config, metadataFields);
    }

    public Expression tablePath() {
        return tablePath;
    }

    public Map<String, Object> config() {
        return config;
    }

    public List<NamedExpression> metadataFields() {
        return metadataFields;
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
        return Objects.hash(source(), tablePath, config, metadataFields, unresolvedMsg);
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
            && Objects.equals(metadataFields, other.metadataFields)
            && Objects.equals(unresolvedMsg, other.unresolvedMsg);
    }

    @Override
    public List<Object> nodeProperties() {
        // config omitted intentionally — SecureString.toString() would leak plaintext into EXPLAIN.
        return singletonList(tablePath);
    }

    @Override
    public String toString() {
        String metadataSuffix = metadataFields.isEmpty() ? "" : " METADATA " + metadataFields;
        return UNRESOLVED_PREFIX + "EXTERNAL[" + tablePath.sourceText() + "]" + metadataSuffix;
    }
}
