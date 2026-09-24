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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;
import static org.elasticsearch.xpack.esql.plan.logical.Highlight.ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS;

// TODO: decide whether HIGHLIGHT should always run on the coordinator. For now we do not force a location in the planner.
// TODO: carry the resolved analyzer name once the "analyzer" option is supported.
public class HighlightExec extends UnaryExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "HighlightExec",
        HighlightExec::readFrom
    );

    private final String prefix;
    private final Expression query;
    private final List<NamedExpression> fields;
    private final MapExpression options;
    private final List<Attribute> generatedFields;
    /** See {@link org.elasticsearch.xpack.esql.plan.logical.Highlight#indexKey()}. */
    private final @Nullable Attribute indexKey;
    /** See {@link org.elasticsearch.xpack.esql.plan.logical.Highlight#fieldMappings()}. */
    private final Map<String, TextEsField> fieldMappings;

    public HighlightExec(
        Source source,
        PhysicalPlan child,
        String prefix,
        Expression query,
        List<NamedExpression> fields,
        MapExpression options,
        List<Attribute> generatedFields,
        @Nullable Attribute indexKey,
        Map<String, TextEsField> fieldMappings
    ) {
        super(source, child);
        this.prefix = prefix;
        this.query = query;
        this.fields = fields;
        this.options = options;
        this.generatedFields = generatedFields;
        this.indexKey = indexKey;
        this.fieldMappings = fieldMappings;
    }

    private static HighlightExec readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        PhysicalPlan child = in.readNamedWriteable(PhysicalPlan.class);
        String prefix = in.readString();
        Expression query = in.readOptionalNamedWriteable(Expression.class);
        List<NamedExpression> fields = in.readNamedWriteableCollectionAsList(NamedExpression.class);
        // MapExpression is registered under the Expression category, not its own, so read it as an Expression.
        MapExpression options = (MapExpression) in.readOptionalNamedWriteable(Expression.class);
        List<Attribute> generatedFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        boolean supportsImplicit = in.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS);
        Attribute indexKey = supportsImplicit ? in.readOptionalNamedWriteable(Attribute.class) : null;
        Map<String, TextEsField> fieldMappings = supportsImplicit ? in.readImmutableMap(EsField::readFrom) : Map.of();
        return new HighlightExec(source, child, prefix, query, fields, options, generatedFields, indexKey, fieldMappings);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeString(prefix);
        out.writeOptionalNamedWriteable(query);
        out.writeNamedWriteableCollection(fields);
        out.writeOptionalNamedWriteable(options);
        out.writeNamedWriteableCollection(generatedFields);
        if (out.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS)) {
            // The analyzer only sets the key and the mappings when every node supports this version.
            out.writeOptionalNamedWriteable(indexKey);
            out.writeMap(fieldMappings, (o, mapping) -> mapping.writeTo(o));
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String prefix() {
        return prefix;
    }

    public Expression query() {
        return query;
    }

    public List<NamedExpression> fields() {
        return fields;
    }

    public MapExpression options() {
        return options;
    }

    public List<Attribute> generatedFields() {
        return generatedFields;
    }

    public Attribute indexKey() {
        return indexKey;
    }

    public Map<String, TextEsField> fieldMappings() {
        return fieldMappings;
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(generatedFields, child().output());
    }

    @Override
    protected AttributeSet computeReferences() {
        // The ON fields and the index key are inputs; the generated <prefix><field> columns are outputs, not references.
        return Expressions.references(indexKey == null ? fields : CollectionUtils.combine(fields, indexKey));
    }

    @Override
    public HighlightExec replaceChild(PhysicalPlan newChild) {
        return new HighlightExec(source(), newChild, prefix, query, fields, options, generatedFields, indexKey, fieldMappings);
    }

    @Override
    protected NodeInfo<HighlightExec> info() {
        return NodeInfo.create(this, HighlightExec::new, child(), prefix, query, fields, options, generatedFields, indexKey, fieldMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prefix, query, fields, options, generatedFields, indexKey, fieldMappings);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        HighlightExec other = (HighlightExec) obj;
        return Objects.equals(prefix, other.prefix)
            && Objects.equals(query, other.query)
            && Objects.equals(fields, other.fields)
            && Objects.equals(options, other.options)
            && Objects.equals(generatedFields, other.generatedFields)
            && Objects.equals(indexKey, other.indexKey)
            && Objects.equals(fieldMappings, other.fieldMappings);
    }
}
