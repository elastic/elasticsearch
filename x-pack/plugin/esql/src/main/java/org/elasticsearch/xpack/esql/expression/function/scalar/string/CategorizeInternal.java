/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer.CloseableTokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Categorizes text messages; is used internally in the Categorize grouping function.
 */
public class CategorizeInternal extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "CategorizeInternal",
        CategorizeInternal::new
    );

    private final Expression str;

    @FunctionInfo(returnType = "integer", description = "Categorizes text messages")
    public CategorizeInternal(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "String expression.") Expression str
    ) {
        super(source, Collections.singletonList(str));
        this.str = str;
    }

    private CategorizeInternal(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(str);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Evaluator
    static int process(
        BytesRef v,
        @Fixed(includeInToString = false, build = true) CategorizationAnalyzer analyzer,
        @Fixed(includeInToString = false, build = true) CloseableTokenListCategorizer categorizer
    ) {
        String s = v.utf8ToString();
        try (TokenStream ts = analyzer.tokenStream("text", s)) {
            return categorizer.computeCategory(ts, s.length(), 1).getId();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        return new CategorizeInternalEvaluator.Factory(
            source(),
            toEvaluator.apply(str),
            context -> new CategorizationAnalyzer(
                // TODO(jan): get the correct analyzer in here, see CategorizationAnalyzerConfig::buildStandardCategorizationAnalyzer
                new CustomAnalyzer(
                    TokenizerFactory.newFactory("whitespace", WhitespaceTokenizer::new),
                    new CharFilterFactory[0],
                    new TokenFilterFactory[0]
                ),
                true
            ),
            context -> new CloseableTokenListCategorizer(
                new CategorizationBytesRefHash(new BytesRefHash(2048, context.bigArrays())),
                CategorizationPartOfSpeechDictionary.getInstance(),
                0.70f
            )
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new CategorizeInternal(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, CategorizeInternal::new, str);
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isString(str(), sourceText(), DEFAULT);
    }

    @Override
    public boolean foldable() {
        return str.foldable();
    }

    Expression str() {
        return str;
    }
}
