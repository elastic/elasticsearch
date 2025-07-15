/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class Untable extends UnaryPlan implements PostAnalysisVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Untable", Untable::new);

    private final List<NamedExpression> sourceColumns;
    private final Attribute keyColumn;
    private final Attribute valueColumn;

    private List<Attribute> output;

    public Untable(Source source, LogicalPlan child, List<NamedExpression> sourceColumns, Attribute keyColumn, Attribute valueColumn) {
        super(source, child);
        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
        this.sourceColumns = sourceColumns;
    }

    private Untable(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(sourceColumns);
        out.writeNamedWriteable(keyColumn);
        out.writeNamedWriteable(valueColumn);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public static List<Attribute> calculateOutput(
        List<Attribute> input,
        List<NamedExpression> sourceColumns,
        Attribute keyColumn,
        Attribute valueColumn
    ) {
        List<String> sourceNames = sourceColumns.stream().map(NamedExpression::name).collect(Collectors.toUnmodifiableList());
        List<Attribute> result = new ArrayList<>();
        for (Attribute attribute : input) {
            String name = attribute.name();
            if ((sourceNames.contains(name) || name.equals(keyColumn.name()) || name.equals(valueColumn)) == false) {
                result.add(attribute);
            }
        }
        result.add(keyColumn);
        result.add(valueColumn);
        return result;
    }

    @Override
    protected AttributeSet computeReferences() {
        Set<Attribute> result = new HashSet<>();
        for (NamedExpression sourceColumn : sourceColumns) {
            result.addAll(sourceColumn.references());
        }
        return AttributeSet.of(result);
    }

    public String commandName() {
        return "UNTABLE";
    }

    @Override
    public boolean expressionsResolved() {
        return sourceColumns.stream().allMatch(Expression::resolved);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Untable(source(), newChild, sourceColumns, keyColumn, valueColumn);
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = calculateOutput(child().output(), sourceColumns, keyColumn, valueColumn);
        }
        return output;
    }

    public List<NamedExpression> sourceColumns() {
        return sourceColumns;
    }

    public Attribute keyColumn() {
        return keyColumn;
    }

    public Attribute valueColumn() {
        return valueColumn;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Untable::new, child(), sourceColumns, keyColumn, valueColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sourceColumns, keyColumn, valueColumn);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        Untable other = ((Untable) obj);
        return Objects.equals(sourceColumns, other.sourceColumns)
            && Objects.equals(keyColumn, other.keyColumn)
            && Objects.equals(valueColumn, other.valueColumn);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        DataType type = null;
        if (sourceColumns.isEmpty()) {
            failures.add(fail(this, "UNTABLE does not match any columns [{}]", sourceText()));
        }
        for (NamedExpression sourceColumn : sourceColumns) {
            DataType nextType = sourceColumn.dataType();
            if (type == null) {
                type = nextType;
            } else if (type != nextType) {
                failures.add(
                    fail(
                        this,
                        "Cannot UNTABLE columns of different types: [{}] type [{}], [{}] type [{}]",
                        sourceColumns.get(0).name(),
                        sourceColumns.get(0).dataType(),
                        sourceColumn.name(),
                        sourceColumn.dataType()
                    )
                );
                return;
            }
        }

    }
}
