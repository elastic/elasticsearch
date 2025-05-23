/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.ScoreOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.score.ScoreMapper;

import java.io.IOException;
import java.util.List;

/**
 * A function to be used to score specific portions of an ES|QL query e.g., in conjunction with
 * an {@link org.elasticsearch.xpack.esql.plan.logical.Eval}.
 */
public class ScoreFunction extends Function implements EvaluatorMapper {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "score",
        ScoreFunction::readFrom
    );

    public static final String NAME = "score";

    @FunctionInfo(
        returnType = "double",
        preview = true,
        description = "Scores a full text function. Returns scores for all the matching docs.",
        examples = { @Example(file = "score-function", tag = "score-single") }
    )
    public ScoreFunction(
        Source source,
        @Param(name = "query", type = { "keyword", "text" }, description = "full text function.") Expression scorableQuery
    ) {
        this(source, List.of(scorableQuery));
    }

    protected ScoreFunction(Source source, List<Expression> children) {
        super(source, children);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ScoreFunction(source(), newChildren);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ScoreFunction::new, children().getFirst());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        ScoreOperator.ExpressionScorer.Factory scorerFactory = ScoreMapper.toScorer(children().getFirst(), toEvaluator.shardContexts());
        return driverContext -> new ScorerEvaluatorFactory(scorerFactory).get(driverContext);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(this.children());
    }

    private static Expression readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression query = in.readOptionalNamedWriteable(Expression.class);
        return new ScoreFunction(source, query);
    }

    private record ScorerEvaluatorFactory(ScoreOperator.ExpressionScorer.Factory scoreFactory)
        implements
            EvalOperator.ExpressionEvaluator.Factory {

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new EvalOperator.ExpressionEvaluator() {

                private final ScoreOperator.ExpressionScorer scorer = scoreFactory.get(context);

                @Override
                public void close() {
                    scorer.close();
                }

                @Override
                public Block eval(Page page) {
                    return scorer.score(page);
                }
            };
        }
    }
}
