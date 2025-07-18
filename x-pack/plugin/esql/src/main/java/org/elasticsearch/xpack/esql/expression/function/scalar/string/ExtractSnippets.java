/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.lucene.LuceneQueryEvaluator;
import org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Extract snippets function, that extracts the most relevant snippets from a given input string
 */
public class ExtractSnippets extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ExtractSnippets",
        ExtractSnippets::new
    );

    private static final int DEFAULT_NUM_SNIPPETS = 1;
    private static final int DEFAULT_SNIPPET_LENGTH = 10; // TODO determine a good default. 512 * 5?

    // TODO better names?
    private final Expression field, str, numSnippets, snippetLength;

    @FunctionInfo(
        returnType = "keyword",
        description = """
            Extracts the most relevant snippets to return from a given input string""",
        examples = @Example(file = "keyword", tag = "extract_snippets")
    )
    public ExtractSnippets(
        Source source,
        @Param(name = "field", type = { "keyword" }, description = "The input string") Expression field,
        @Param(name = "str", type = { "keyword", "text" }, description = "The input string") Expression str,
        @Param(
            optional = true,
            name = "num_snippets",
            type = { "integer" },
            description = "The number of snippets to return. Defaults to " + DEFAULT_NUM_SNIPPETS
        ) Expression numSnippets,
        @Param(
            optional = true,
            name = "snippet_length",
            type = { "integer" },
            description = "The length of snippets to return. Defaults to " + DEFAULT_SNIPPET_LENGTH
        ) Expression snippetLength
    ) {
        super(source, numSnippets == null ? Collections.singletonList(str) : Arrays.asList(str, numSnippets));
        this.field = field;
        this.str = str;
        this.numSnippets = numSnippets;
        this.snippetLength = snippetLength;
    }

    private ExtractSnippets(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(str);
        out.writeOptionalNamedWriteable(numSnippets);
        out.writeOptionalNamedWriteable(snippetLength);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return field.dataType().noText();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(field, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isString(str, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = numSnippets == null
            ? TypeResolution.TYPE_RESOLVED
            : isType(numSnippets, dt -> dt == DataType.INTEGER, sourceText(), THIRD, "integer");
        if (resolution.unresolved()) {
            return resolution;
        }

        return snippetLength == null
            ? TypeResolution.TYPE_RESOLVED
            : isType(numSnippets, dt -> dt == DataType.INTEGER, sourceText(), FOURTH, "integer");
    }

    @Override
    public boolean foldable() {
        return field.foldable()
            && str.foldable()
            && (numSnippets == null || numSnippets.foldable())
            && (snippetLength == null || snippetLength.foldable());
    }

    // @Evaluator
    // static BytesRef process(BytesRef field, BytesRef str, int numSnippets, int snippetLength) {
    // if (field == null || field.length == 0 || str == null || str.length == 0) {
    // return null;
    // }
    //
    // String utf8Field = field.utf8ToString();
    // String utf8Str = str.utf8ToString();
    // if (snippetLength > utf8Field.length()) {
    // return field;
    // }
    //
    // // TODO - actually calculate snippets using search string, this truncation is just a placeholder
    // List<String> snippets = new ArrayList<>(numSnippets);
    // int pos = 0;
    // for (int i = 0; i < numSnippets && pos < utf8Field.length(); i++) {
    // int end = Math.min(pos + snippetLength, utf8Field.length());
    // String snippet = utf8Field.substring(pos, end);
    // snippets.add(snippet);
    // pos += snippetLength;
    // }
    // return snippets.get(0);
    // }
    //
    // @Evaluator(extraName = "NoStart")
    // static BytesRef process(BytesRef field, BytesRef str) {
    // return process(field, str, DEFAULT_NUM_SNIPPETS, DEFAULT_SNIPPET_LENGTH);
    // }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ExtractSnippets(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            numSnippets == null ? null : newChildren.get(1),
            snippetLength == null ? null : newChildren.get(2)
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ExtractSnippets::new, field, str, numSnippets, snippetLength);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<EsPhysicalOperationProviders.ShardContext> shardContexts = toEvaluator.shardContexts();
        LuceneQueryEvaluator.ShardConfig[] shardConfigs = new LuceneQueryEvaluator.ShardConfig[shardContexts.size()];
        int i = 0;
        for (EsPhysicalOperationProviders.ShardContext shardContext : shardContexts) {
            shardConfigs[i++] = new LuceneQueryEvaluator.ShardConfig(shardContext.toQuery(queryBuilder()), shardContext.searcher());
        }
        return new LuceneQueryExpressionEvaluator.Factory(shardConfigs);
    }

    }

    Expression str() {
        return str;
    }

    Expression numSnippets() {
        return numSnippets;
    }

    Expression snippetLength() {
        return snippetLength;
    }
}
