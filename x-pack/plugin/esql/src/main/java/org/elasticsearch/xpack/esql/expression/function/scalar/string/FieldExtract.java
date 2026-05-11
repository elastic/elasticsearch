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
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
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
 * Extracts a single sub-field from a {@code flattened} field root as {@code keyword}.
 * <p>
 *     The second argument is the <em>literal</em> name of a flattened sub-field, that is,
 *     exactly the dotted key as it is stored in doc values for the flattened root (for example
 *     {@code "host.name"}).
 * </p>
 */
public class FieldExtract extends EsqlScalarFunction {
    private static final BytesRef TRUE_BYTES = new BytesRef("true");
    private static final BytesRef FALSE_BYTES = new BytesRef("false");

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
            Extracts the value of a single sub-field from a [`flattened` field](/reference/elasticsearch/mapping-reference/flattened.md) \
            root as `keyword`.""",
        detailedDescription = """
            The first argument must be a field whose ES mapping type is `flattened` (the root of the flattened object).
            The second argument is the *literal* name of the sub-field to extract, that is, exactly the dotted key as
            stored in doc values for the flattened root. For example, `field_extract(resource.attributes, "host.name")`
            looks up the literal storage key `host.name`. The dot is part of the key, not a path separator. Nested
            objects in the original document also collapse to dotted keys (an input `{"a":{"b":"x"}}` is stored as the
            flat key `a.b`), so the same dotted form addresses both flat and originally-nested sub-fields.

            JSONPath syntax is not supported: brackets (`['host.name']`) and array indices (`tags[0]`) are rejected.
            Path matching is case-sensitive.

            Returns `null` if either argument is `null`, if no sub-field with that name exists, or if the stored value
            is JSON `null`. Returns `null` and emits a warning if the root value is not valid JSON.

            String values are returned without surrounding quotes, numbers and booleans as their string representation,
            and objects and arrays as JSON strings. When the sub-field is multi-valued in the flattened field, the
            result is a multi-valued `keyword` block.""",
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
            description = "Literal name of the flattened sub-field to extract (e.g. `\"host.name\"`). "
                + "Brackets and array indices are not supported. If `null`, the function returns `null`."
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
                    validateFieldExtractPath(((BytesRef) foldedPath).utf8ToString());
                } catch (IllegalArgumentException e) {
                    return new TypeResolution(e.getMessage());
                }
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Verifies that {@code path} is a usable literal flattened sub-field name: non-empty and free of
     * JSONPath syntax. The {@code [} and {@code ]} characters are rejected to catch both array
     * indices ({@code tags[0]}) and quoted bracket keys ({@code ['host.name']}). Neither has any
     * meaning for flattened storage where every leaf is addressed by its dotted key.
     */
    static void validateFieldExtractPath(String path) {
        if (path.isEmpty()) {
            throw new IllegalArgumentException("field_extract path must not be empty");
        }
        if (path.indexOf('[') >= 0 || path.indexOf(']') >= 0) {
            throw new IllegalArgumentException(
                "field_extract path must be a literal flattened sub-field name. Brackets and array indices are not supported"
            );
        }
    }

    @Override
    public boolean foldable() {
        return field.foldable() && path.foldable();
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    public static void process(BytesRefBlock.Builder builder, BytesRef flattenedJson, BytesRef path) {
        String key = path.utf8ToString();
        validateFieldExtractPath(key);
        extractTopLevelKey(builder, flattenedJson, key);
    }

    @Evaluator(extraName = "Constant", warnExceptions = IllegalArgumentException.class)
    static void processConstant(BytesRefBlock.Builder builder, BytesRef flattenedJson, @Fixed String path) {
        // path was validated at plan time, no need to re-check per row.
        extractTopLevelKey(builder, flattenedJson, path);
    }

    private static void extractTopLevelKey(BytesRefBlock.Builder builder, BytesRef str, String key) {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, str.bytes, str.offset, str.length)
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("path [" + key + "] does not exist");
            }
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String name = parser.currentName();
                    parser.nextToken();
                    if (name.equals(key)) {
                        builder.appendBytesRef(new BytesRef(parser.text()));
                        return;
                    }
                    parser.skipChildren();
                }
            }
            throw new IllegalArgumentException("path [" + key + "] does not exist");
        } catch (IOException | XContentParseException e) {
            throw new IllegalArgumentException("invalid JSON input");
        }
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
            String pathString = ((BytesRef) path.fold(toEvaluator.foldCtx())).utf8ToString();
            validateFieldExtractPath(pathString);
            return new FieldExtractConstantEvaluator.Factory(source(), fieldEval, pathString);
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
