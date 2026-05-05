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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
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
 * Extracts a single sub-key from a {@code flattened} field root as {@code keyword}.
 * <p>
 *     The second argument is a <strong>field path</strong>, not full JSONPath: it may use dot notation and
 *     quoted bracket notation for keys (same subset as {@link JsonExtract} for navigation), but
 *     <strong>array indices are not allowed</strong> — flattened sub-keys are addressed as field paths only.
 * </p>
 */
public class FieldExtract extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FieldExtract",
        FieldExtract::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FieldExtract.class)
        .binary(FieldExtract::new)
        .name("field_extract");

    private final Expression field;
    private final Expression path;

    @FunctionInfo(
        returnType = "keyword",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.5.0") },
        description = """
            Extracts the value of a single sub-key from a `flattened` field root as `keyword`.""",
        detailedDescription = """
            The first argument must be a field whose ES mapping type is `flattened` (the root of the flattened object).
            The second argument is the path to the sub-key inside the JSON blob produced for that root, using the same
            dot and quoted-bracket rules as `JSON_EXTRACT` for navigating objects, except that array indices are not
            supported — `field_extract` addresses flattened storage as field paths, not arbitrary JSONPath.
            Doc values for a `flattened` root are often emitted as a single JSON object whose keys are full dotted paths
            (for example `host.name`); use quoted bracket segments such as `['host.name']` instead of `host.name`, which
            would look for a nested object `host` then key `name`.

            Returns `null` if either argument is `null`, if the path does not exist, or if the extracted JSON value is
            `null`. Returns `null` and emits a warning if the root value is not valid JSON.

            String values are returned without surrounding quotes; numbers and booleans as their string representation;
            objects and arrays as JSON strings. When the sub-key is multi-valued in the flattened field, the result is
            a multi-valued `keyword` block.""",
        examples = @Example(file = "field_extract", tag = "field_extract_host_name")
    )
    public FieldExtract(
        Source source,
        @Param(
            name = "field",
            type = { "flattened" },
            description = "The root of a `flattened` mapping field. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            name = "path",
            type = { "keyword", "text" },
            description = "Path to the sub-key to extract, using dot and quoted-bracket notation (no array indices). "
                + "If `null`, the function returns `null`."
        ) Expression path
    ) {
        super(source, List.of(field, path));
        this.field = field;
        this.path = path;
    }

    private FieldExtract(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
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

        TypeResolution resolution = isType(field, t -> t == DataType.FLATTENED, sourceText(), FIRST, "flattened");
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isString(path, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (path.foldable()) {
            var foldedPath = path.fold(FoldContext.small());
            if (foldedPath != null) {
                try {
                    String pathString = ((BytesRef) foldedPath).utf8ToString();
                    JsonPath jsonPath = JsonPath.parse(pathString);
                    validateFieldExtractPath(jsonPath);
                } catch (IllegalArgumentException e) {
                    return new TypeResolution(e.getMessage());
                }
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Ensures the path does not use array segments (flattened field paths are object keys only).
     */
    static void validateFieldExtractPath(JsonPath path) {
        if (path.segments().isEmpty()) {
            throw new IllegalArgumentException("field_extract path must contain at least one key segment");
        }
        for (JsonPath.Segment segment : path.segments()) {
            if (segment instanceof JsonPath.Segment.Index) {
                throw new IllegalArgumentException("field_extract path cannot use array indices");
            }
        }
    }

    @Override
    public boolean foldable() {
        return field.foldable() && path.foldable();
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    public static void process(BytesRefBlock.Builder builder, BytesRef flattenedJson, BytesRef path) {
        JsonPath jsonPath = JsonPath.parse(path.utf8ToString());
        validateFieldExtractPath(jsonPath);
        JsonPathValueExtractor.extract(builder, flattenedJson, jsonPath);
    }

    @Evaluator(extraName = "Constant", warnExceptions = IllegalArgumentException.class)
    static void processConstant(BytesRefBlock.Builder builder, BytesRef flattenedJson, @Fixed JsonPath path) {
        validateFieldExtractPath(path);
        JsonPathValueExtractor.extract(builder, flattenedJson, path);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FieldExtract(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FieldExtract::new, field, path);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory fieldEval = toEvaluator.apply(field);
        if (path.foldable()) {
            JsonPath jsonPath = JsonPath.parse(((BytesRef) path.fold(toEvaluator.foldCtx())).utf8ToString());
            validateFieldExtractPath(jsonPath);
            return new FieldExtractConstantEvaluator.Factory(source(), fieldEval, jsonPath);
        }
        ExpressionEvaluator.Factory pathEval = toEvaluator.apply(path);
        return new FieldExtractEvaluator.Factory(source(), fieldEval, pathEval);
    }

    Expression field() {
        return field;
    }

    Expression path() {
        return path;
    }
}
