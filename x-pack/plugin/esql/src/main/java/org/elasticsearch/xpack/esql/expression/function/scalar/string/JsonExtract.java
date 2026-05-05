/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Extracts a value from a JSON string using a {@link JsonPath} subset.
 * Preview / snapshot-only, gated behind {@code FN_JSON_EXTRACT}.
 */
public class JsonExtract extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "JsonExtract",
        JsonExtract::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(JsonExtract.class)
        .binary(JsonExtract::new)
        .name("json_extract");

    private final Expression str;
    private final Expression path;

    @FunctionInfo(
        returnType = "keyword",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") },
        description = """
            Extracts a value from a JSON string using a subset of
            [JSONPath](https://datatracker.ietf.org/doc/rfc9535) syntax.""",
        detailedDescription = """
            Paths can use dot notation (`user.address.city`), bracket
            notation (`['user']['address']['city']`), or a mix of both
            (`user['address'].city`). For simple keys, dot notation and
            bracket notation are interchangeable — `a.b` and `a['b']`
            produce the same result.

            Bracket notation is required for keys that contain dots or
            special characters (`['user.name']`), for empty string keys
            (`['']`), and for array indices (`items[0]`). Dots in dot
            notation are always path separators per the JSONPath
            specification — a JSON key that literally contains a dot
            (e.g., `"user.name"`) must be accessed via bracket notation.
            The JSONPath `$` root selector is supported for compatibility
            but is always optional — `$.name` and `name` are equivalent,
            and `$[0]` and `[0]` are equivalent. Optional whitespace is
            allowed inside brackets (`[ 0 ]` is equivalent to `[0]`).
            Path matching is case-sensitive per the JSON specification.

            The extracted value is returned as a `keyword` string: string
            values without surrounding quotes, numbers and booleans as their
            string representation, and objects or arrays as JSON strings.
            Returns `null` if either parameter is `null` or if the extracted
            JSON value is `null`.

            Returns `null` and emits a warning if the input is not valid JSON,
            the path is malformed, the path does not exist, the array index is
            out of bounds, or the path attempts to traverse through a
            non-object/non-array value.

            This function does not support wildcards (`*`), recursive descent
            (`..`), array slicing (`[0:3]`), filter expressions
            (`?(@.price<10)`), or negative array indices (`[-1]`).""",
        examples = {
            @Example(file = "json_extract", tag = "json_extract"),
            @Example(file = "json_extract", tag = "json_extract_dollar", description = """
                The `$` prefix is optional — this query produces the same result as the previous example:"""),
            @Example(
                file = "json_extract",
                tag = "json_extract_nested",
                description = "To extract a deeply nested value, use dot-notation:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_bracket",
                description = "Keys that contain dots (common in OpenTelemetry semantic conventions) "
                    + "require quoted bracket notation — here `service.name` is a single key, not a nested path:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_array",
                description = "Array indices can be combined with dot notation to navigate arrays of objects:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_object",
                description = "When the extracted value is an object or array, it is returned as a JSON string:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_top_level_array",
                description = "To extract from a top-level JSON array, use a bracket index on the root element:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_deep_nesting",
                description = "Dot notation, array indices, and object keys can be combined to navigate deeply nested structures:"
            ), }
    )
    public JsonExtract(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text", "_source" },
            description = "A string containing valid JSON, or the `_source` field. If `null`, the function returns `null`."
        ) Expression str,
        @Param(
            name = "path",
            type = { "keyword", "text" },
            description = "A path expression identifying the value to extract, using a subset of "
                + "JSONPath syntax. Supports dot notation (`user.name`), bracket notation for "
                + "array indices (`items[0]`), and quoted brackets for keys with special characters "
                + "(`['user.name']`). The `$` prefix is optional. "
                + "If `null`, the function returns `null`."
        ) Expression path
    ) {
        super(source, List.of(str, path));
        this.str = str;
        this.path = path;
    }

    private JsonExtract(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(str);
        out.writeNamedWriteable(path);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(
            str,
            t -> DataType.isString(t) || t == DataType.SOURCE,
            sourceText(),
            FIRST,
            "keyword",
            "text",
            "_source"
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isString(path, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return str.foldable() && path.foldable();
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    public static void process(BytesRefBlock.Builder builder, BytesRef str, BytesRef path) {
        JsonPathValueExtractor.extract(builder, str, JsonPath.parse(path.utf8ToString()));
    }

    @Evaluator(extraName = "Constant", warnExceptions = IllegalArgumentException.class)
    static void processConstant(BytesRefBlock.Builder builder, BytesRef str, @Fixed JsonPath path) {
        JsonPathValueExtractor.extract(builder, str, path);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new JsonExtract(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, JsonExtract::new, str, path);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory strExpr = toEvaluator.apply(str);
        if (path.foldable()) {
            JsonPath jsonPath = JsonPath.parse(((BytesRef) path.fold(toEvaluator.foldCtx())).utf8ToString());
            return new JsonExtractConstantEvaluator.Factory(source(), strExpr, jsonPath);
        }
        ExpressionEvaluator.Factory pathExpr = toEvaluator.apply(path);
        return new JsonExtractEvaluator.Factory(source(), strExpr, pathExpr);
    }

    Expression str() {
        return str;
    }

    Expression path() {
        return path;
    }
}
