/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.DatasetReference;

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
 * @see UnresolvedSourceRelation marker implemented by both shapes; match on it to traverse any
 * FROM-style leaf.
 */
public final class UnresolvedExternalRelation extends LeafPlan implements Unresolvable, UnresolvedSourceRelation {

    private final Expression tablePath;
    private final Map<String, Object> config;
    private final List<NamedExpression> metadataFields;
    @Nullable
    private final DatasetReference datasetReference;
    @Nullable
    private final DatasetMapping mapping;
    private final String unresolvedMsg;

    /**
     * Creates an unresolved external relation with no METADATA fields. Convenience overload for
     * callers that don't carry standard metadata names (the inline {@code EXTERNAL} command path,
     * tests).
     */
    public UnresolvedExternalRelation(Source source, Expression tablePath, Map<String, Object> config) {
        this(source, tablePath, config, List.of(), null);
    }

    /**
     * Creates an unresolved external relation without a backing dataset name (the inline
     * {@code EXTERNAL} command path or tests that exercise the analyzer with a bare path).
     */
    public UnresolvedExternalRelation(
        Source source,
        Expression tablePath,
        Map<String, Object> config,
        List<NamedExpression> metadataFields
    ) {
        this(source, tablePath, config, metadataFields, null);
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
     * <p>
     * {@code datasetReference} says which dataset is read: a registered name, when
     * {@link org.elasticsearch.xpack.esql.datasources.DatasetRewriter} created the relation from a
     * {@code FROM <dataset>} pattern, or a definition the query supplied. It is {@code null} for the inline
     * {@code EXTERNAL} command path, which has no dataset behind it at all. The name arm reaches the operator
     * factory so the per-file {@code _index} synthesizer can emit it rather than the raw resource path.
     *
     * @param source the source location in the query
     * @param tablePath the resource path or external table identifier (a {@code Literal} or parameter reference)
     * @param config plain-valued configuration (e.g., credentials, format options) — not wrapped in {@code Literal}
     * @param metadataFields names requested in the {@code METADATA} clause, in declaration order; never {@code null}
     * @param datasetReference the dataset this leaf reads, named or defined inline; {@code null} when neither
     */
    public UnresolvedExternalRelation(
        Source source,
        Expression tablePath,
        Map<String, Object> config,
        List<NamedExpression> metadataFields,
        @Nullable DatasetReference datasetReference
    ) {
        this(source, tablePath, config, metadataFields, datasetReference, null);
    }

    /**
     * Full constructor carrying the dataset's user-declared mapping ({@link DatasetMapping}), if any. Only the
     * {@code FROM <dataset>} path (via {@code DatasetRewriter}) supplies a non-null mapping; the inline
     * {@code EXTERNAL} path and tests pass {@code null} (inference only). The mapping is consumed in pre-analysis
     * by the external-source resolver to drive the strict-skip / non-strict-overlay decision.
     */
    public UnresolvedExternalRelation(
        Source source,
        Expression tablePath,
        Map<String, Object> config,
        List<NamedExpression> metadataFields,
        @Nullable DatasetReference datasetReference,
        @Nullable DatasetMapping mapping
    ) {
        super(source);
        this.tablePath = tablePath;
        this.config = config;
        this.metadataFields = Objects.requireNonNull(metadataFields, "metadataFields");
        this.datasetReference = datasetReference;
        this.mapping = mapping;
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
        return NodeInfo.create(this, UnresolvedExternalRelation::new, tablePath, config, metadataFields, datasetReference, mapping);
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

    /**
     * Which dataset this relation reads — a registered name, or a definition the query supplied. {@code null} when
     * there is no dataset behind the source at all, which only the inline {@code EXTERNAL} command plans.
     */
    @Nullable
    public DatasetReference datasetReference() {
        return datasetReference;
    }

    /**
     * The dataset's user-declared mapping (the `mappings` block), or {@code null} when the dataset has none
     * (inference only) or for the inline {@code EXTERNAL} path. Consumed by the external-source resolver.
     */
    @Nullable
    public DatasetMapping mapping() {
        return mapping;
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
        // No source(): equals() below ignores it, and the equals/hashCode contract requires equal
        // nodes to hash equal (same node parsed at two positions must collapse in plan-node sets).
        return Objects.hash(tablePath, config, metadataFields, datasetReference, mapping, unresolvedMsg);
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
            && Objects.equals(datasetReference, other.datasetReference)
            && Objects.equals(mapping, other.mapping)
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
