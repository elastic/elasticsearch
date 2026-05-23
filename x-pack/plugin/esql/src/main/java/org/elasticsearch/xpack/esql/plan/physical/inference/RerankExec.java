/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class RerankExec extends InferenceExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "RerankExec",
        RerankExec::new
    );

    private final Expression queryText;
    private final List<Alias> rerankFields;
    private final Attribute scoreAttribute;
    private final TimeValue timeout;
    private List<Attribute> lazyOutput;

    public RerankExec(
        Source source,
        PhysicalPlan child,
        Expression inferenceId,
        Expression queryText,
        List<Alias> rerankFields,
        Attribute scoreAttribute
    ) {
        this(source, child, inferenceId, queryText, rerankFields, scoreAttribute, null);
    }

    public RerankExec(
        Source source,
        PhysicalPlan child,
        Expression inferenceId,
        Expression queryText,
        List<Alias> rerankFields,
        Attribute scoreAttribute,
        TimeValue timeout
    ) {
        super(source, child, inferenceId);
        this.queryText = queryText;
        this.rerankFields = rerankFields;
        this.scoreAttribute = scoreAttribute;
        this.timeout = timeout;
    }

    public RerankExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readCollectionAsList(Alias::new),
            in.readNamedWriteable(Attribute.class),
            in.getTransportVersion().supports(InferencePlan.ESQL_INFERENCE_ACCEPT_TIMEOUT) ? in.readOptionalTimeValue() : null
        );
    }

    public Expression queryText() {
        return queryText;
    }

    public List<Alias> rerankFields() {
        return rerankFields;
    }

    public Attribute scoreAttribute() {
        return scoreAttribute;
    }

    public TimeValue timeout() {
        return timeout;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(queryText());
        out.writeCollection(rerankFields());
        out.writeNamedWriteable(scoreAttribute);
        if (out.getTransportVersion().supports(InferencePlan.ESQL_INFERENCE_ACCEPT_TIMEOUT)) {
            out.writeOptionalTimeValue(timeout);
        }
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, RerankExec::new, child(), inferenceId(), queryText, rerankFields, scoreAttribute, timeout);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new RerankExec(source(), newChild, inferenceId(), queryText, rerankFields, scoreAttribute, timeout);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(List.of(scoreAttribute), child().output());
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return Rerank.computeReferences(rerankFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        RerankExec rerank = (RerankExec) o;
        return Objects.equals(queryText, rerank.queryText)
            && Objects.equals(rerankFields, rerank.rerankFields)
            && Objects.equals(scoreAttribute, rerank.scoreAttribute)
            && Objects.equals(timeout, rerank.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queryText, rerankFields, scoreAttribute, timeout);
    }
}
