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
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Eval extends UnaryPlan implements GeneratingPlan<Eval>, PostAnalysisVerificationAware, TelemetryAware, SortAgnostic {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Eval", Eval::new);

    private final List<Alias> fields;
    private List<Attribute> lazyOutput;

    public Eval(Source source, LogicalPlan child, List<Alias> fields) {
        super(source, child);
        this.fields = fields;
    }

    private Eval(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(LogicalPlan.class), in.readCollectionAsList(Alias::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(fields());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public List<Alias> fields() {
        return fields;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(fields, child().output());
        }

        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return computeReferences(fields);
    }

    public static AttributeSet computeReferences(List<Alias> fields) {
        AttributeSet generated = AttributeSet.of(asAttributes(fields));
        return Expressions.references(fields).subtract(generated);
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return asAttributes(fields);
    }

    @Override
    public Eval withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);

        return new Eval(source(), child(), renameAliases(fields, newNames));
    }

    private List<Alias> renameAliases(List<Alias> originalAttributes, List<String> newNames) {
        AttributeMap.Builder<Attribute> aliasReplacedByBuilder = AttributeMap.builder();
        List<Alias> newFields = new ArrayList<>(originalAttributes.size());
        for (int i = 0; i < originalAttributes.size(); i++) {
            Alias field = originalAttributes.get(i);
            String newName = newNames.get(i);
            if (field.name().equals(newName)) {
                newFields.add(field);
            } else {
                Alias newField = new Alias(field.source(), newName, field.child(), new NameId(), field.synthetic());
                newFields.add(newField);
                aliasReplacedByBuilder.put(field.toAttribute(), newField.toAttribute());
            }
        }
        AttributeMap<Attribute> aliasReplacedBy = aliasReplacedByBuilder.build();

        // We need to also update any references to the old attributes in the new attributes; e.g.
        // EVAL x = 1, y = x + 1
        // renaming x, y to x1, y1
        // so far became
        // EVAL x1 = 1, y1 = x + 1
        // - but x doesn't exist anymore, so replace it by x1 to obtain
        // EVAL x1 = 1, y1 = x1 + 1

        List<Alias> newFieldsWithUpdatedRefs = new ArrayList<>(originalAttributes.size());
        for (Alias newField : newFields) {
            newFieldsWithUpdatedRefs.add((Alias) newField.transformUp(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r)));
        }

        return newFieldsWithUpdatedRefs;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(fields);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Eval(source(), newChild, fields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Eval::new, child(), fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Eval eval = (Eval) o;
        return child().equals(eval.child()) && Objects.equals(fields, eval.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        fields.forEach(field -> {
            // check supported types
            DataType dataType = field.dataType();
            if (DataType.isRepresentable(dataType) == false) {
                failures.add(
                    fail(
                        field,
                        "EVAL does not support type [{}] as the return data type of expression [{}]",
                        dataType.typeName(),
                        field.child().sourceText()
                    )
                );
            }
        });
    }
}
