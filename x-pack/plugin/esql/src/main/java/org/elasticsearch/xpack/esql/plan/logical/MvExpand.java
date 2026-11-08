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
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MvExpand extends UnaryPlan implements TelemetryAware, SortAgnostic, DocPreserving, PostAnalysisVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "MvExpand", MvExpand::new);

    private final NamedExpression target;
    private final Attribute expanded;

    private List<Attribute> output;

    public MvExpand(Source source, LogicalPlan child, NamedExpression target, Attribute expanded) {
        super(source, child);
        this.target = target;
        this.expanded = expanded;
    }

    private MvExpand(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(NamedExpression.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(target());
        out.writeNamedWriteable(expanded());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public static List<Attribute> calculateOutput(List<Attribute> input, NamedExpression target, Attribute expanded) {
        List<Attribute> result = new ArrayList<>();
        for (Attribute attribute : input) {
            if (attribute.name().equals(target.name())) {
                result.add(expanded);
            } else {
                result.add(attribute);
            }
        }
        return result;
    }

    public NamedExpression target() {
        return target;
    }

    public Attribute expanded() {
        return expanded;
    }

    @Override
    protected AttributeSet computeReferences() {
        return target.references();
    }

    public String telemetryLabel() {
        return "MV_EXPAND";
    }

    @Override
    public boolean expressionsResolved() {
        return target.resolved();
    }

    @Override
    public MvExpand replaceChild(LogicalPlan newChild) {
        return new MvExpand(source(), newChild, target, expanded);
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = calculateOutput(child().output(), target, expanded);
        }
        return output;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, MvExpand::new, child(), target, expanded);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), target, expanded);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        MvExpand other = ((MvExpand) obj);
        return Objects.equals(target, other.target) && Objects.equals(expanded, other.expanded);
    }

    /**
     * A union column cannot be expanded. {@code MV_EXPAND} re-mints the expanded column as a reference, and a
     * reference carries no {@code UnionEsField}, so the legs are gone while the type stays {@code UNION} - and a
     * union whose legs are unknown has no way to say what a value's tag means. Measured, the column came back as
     * five nulls, which is the one outcome worse than an error because nothing says the answer is wrong.
     * <p>
     * Refused rather than fixed here because the fix is not local: the leg list has to travel with any attribute of
     * this type, not only with a field.
     * </p>
     */
    @Override
    public void postAnalysisVerification(Failures failures) {
        if (target().dataType() == DataType.UNION) {
            failures.add(
                Failure.fail(
                    target(),
                    "Column [{}] holds more than one type, so it cannot be expanded. Select one type first "
                        + "[TYPED({}, \"<type>\")], or cast it [{}::<type>]",
                    target().name(),
                    target().name(),
                    target().name()
                )
            );
        }
    }

}
