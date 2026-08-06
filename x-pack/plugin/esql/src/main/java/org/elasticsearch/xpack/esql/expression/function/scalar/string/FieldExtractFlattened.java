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
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.XContentUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class FieldExtractFlattened extends EsqlScalarFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FieldExtractFlattened",
        FieldExtractFlattened::new
    );

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FieldExtractFlattened.class)
        .ternary(FieldExtractFlattened::new)
        .capabilities("returns_multi_value")
        .name("field_extract_flattened");

    private final Expression field;
    private final Expression path;
    private final Expression injectedKey;

    @FunctionInfo(
        returnType = "flattened",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.5.0") },
        briefSummary = "Extracts sub-fields from a flattened field, grouped into multiple flattened objects.",
        description = "Extracts sub-fields from a flattened field, grouped into multiple flattened objects.",
        examples = @Example(file = "field_extract_flattened", tag = "field_extract_flattened_example")
    )
    public FieldExtractFlattened(
        Source source,
        @Param(name = "field", type = { "flattened" }, description = "The flattened field.") Expression field,
        @Param(name = "path", type = { "keyword", "text" }, description = "The path prefix to extract.") Expression path,
        @Param(
            optional = true,
            name = "injected_key",
            type = { "keyword", "text" },
            description = "Key to inject the group identifier into."
        ) Expression injectedKey
    ) {
        super(source, injectedKey == null ? Arrays.asList(field, path) : Arrays.asList(field, path, injectedKey));
        this.field = field;
        this.path = path;
        this.injectedKey = injectedKey;
    }

    public FieldExtractFlattened(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public DataType dataType() {
        return DataType.FLATTENED;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(path);
        out.writeOptionalNamedWriteable(injectedKey);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FieldExtractFlattened::new, field, path, injectedKey);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() == 2) {
            return new FieldExtractFlattened(source(), newChildren.get(0), newChildren.get(1), null);
        }
        return new FieldExtractFlattened(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public boolean foldable() {
        return field.foldable() && path.foldable() && (injectedKey == null || injectedKey.foldable());
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
        if (injectedKey != null) {
            resolution = isString(injectedKey, sourceText(), THIRD);
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory fieldEval = toEvaluator.apply(field);
        ExpressionEvaluator.Factory pathEval = toEvaluator.apply(path);

        if (injectedKey == null) {
            if (path.foldable()) {
                String pathString = ((BytesRef) path.fold(toEvaluator.foldCtx())).utf8ToString();
                return new FieldExtractFlattenedNoKeyConstantEvaluator.Factory(source(), fieldEval, pathString);
            }
            return new FieldExtractFlattenedNoKeyEvaluator.Factory(source(), fieldEval, pathEval);
        } else {
            ExpressionEvaluator.Factory keyEval = toEvaluator.apply(injectedKey);
            if (path.foldable() && injectedKey.foldable()) {
                String pathString = ((BytesRef) path.fold(toEvaluator.foldCtx())).utf8ToString();
                String keyString = ((BytesRef) injectedKey.fold(toEvaluator.foldCtx())).utf8ToString();
                return new FieldExtractFlattenedConstantEvaluator.Factory(source(), fieldEval, pathString, keyString);
            }
            return new FieldExtractFlattenedEvaluator.Factory(source(), fieldEval, pathEval, keyEval);
        }
    }

    @Evaluator(extraName = "NoKey", warnExceptions = IllegalArgumentException.class)
    public static void process(BytesRefBlock.Builder builder, BytesRef flattenedJson, BytesRef path) {
        extract(builder, flattenedJson, path.utf8ToString(), null);
    }

    @Evaluator(extraName = "NoKeyConstant", warnExceptions = IllegalArgumentException.class)
    public static void process(BytesRefBlock.Builder builder, BytesRef flattenedJson, @Fixed String path) {
        extract(builder, flattenedJson, path, null);
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    public static void process(BytesRefBlock.Builder builder, BytesRef flattenedJson, BytesRef path, BytesRef injectedKey) {
        extract(builder, flattenedJson, path.utf8ToString(), injectedKey.utf8ToString());
    }

    @Evaluator(extraName = "Constant", warnExceptions = IllegalArgumentException.class)
    public static void process(BytesRefBlock.Builder builder, BytesRef flattenedJson, @Fixed String path, @Fixed String injectedKey) {
        extract(builder, flattenedJson, path, injectedKey);
    }

    private static void extract(BytesRefBlock.Builder builder, BytesRef str, String prefix, String injectedKey) {
        assert FlattenedFieldMapper.assertSortedKeys(str);

        String prefixDot = prefix.endsWith(".") ? prefix : prefix + ".";

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, str.bytes, str.offset, str.length)
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                builder.appendNull();
                return;
            }

            String currentGroup = null;
            TreeMap<BytesRef, Object> currentGroupFields = new TreeMap<>();
            List<BytesRef> resultJsonStrings = new ArrayList<>();

            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String name = parser.currentName();
                    parser.nextToken(); // move to value

                    if (name.startsWith(prefixDot)) {
                        String remainder = name.substring(prefixDot.length());
                        int dotIndex = remainder.indexOf('.');
                        String group;
                        String innerKey;
                        if (dotIndex != -1) {
                            group = remainder.substring(0, dotIndex);
                            innerKey = remainder.substring(dotIndex + 1);
                        } else {
                            continue; // skip, must have a sub-key
                        }

                        if (currentGroup == null) {
                            currentGroup = group;
                        } else if (!currentGroup.equals(group)) {
                            finalizeGroup(resultJsonStrings, currentGroup, currentGroupFields, injectedKey);
                            currentGroup = group;
                            currentGroupFields.clear();
                        }

                        Object parsedValue = XContentUtils.readValue(parser, parser.currentToken());
                        currentGroupFields.put(new BytesRef(innerKey), parsedValue);
                    } else {
                        parser.skipChildren();
                    }
                }
            }

            if (currentGroup != null) {
                finalizeGroup(resultJsonStrings, currentGroup, currentGroupFields, injectedKey);
            }

            if (resultJsonStrings.isEmpty()) {
                builder.appendNull();
            } else if (resultJsonStrings.size() == 1) {
                builder.appendBytesRef(resultJsonStrings.get(0));
            } else {
                builder.beginPositionEntry();
                for (BytesRef r : resultJsonStrings) {
                    builder.appendBytesRef(r);
                }
                builder.endPositionEntry();
            }

        } catch (IOException | XContentParseException e) {
            throw new IllegalArgumentException("invalid JSON input");
        }
    }

    private static void finalizeGroup(List<BytesRef> resultJsonStrings, String group, TreeMap<BytesRef, Object> fields, String injectedKey)
        throws IOException {
        if (injectedKey != null) {
            fields.put(new BytesRef(injectedKey), group);
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); XContentBuilder builder = XContentFactory.jsonBuilder(baos)) {
            builder.startObject();
            for (Map.Entry<BytesRef, Object> entry : fields.entrySet()) {
                builder.field(entry.getKey().utf8ToString(), entry.getValue());
            }
            builder.endObject();
            builder.flush();

            BytesRef result = new BytesRef(baos.toByteArray());
            assert FlattenedFieldMapper.assertSortedKeys(result);
            resultJsonStrings.add(result);
        }
    }
}
