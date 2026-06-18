/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.ExtractFlattenedSubfieldConfig;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
 * <p>
 *     When the path is a foldable literal key on a real {@code flattened} {@link FieldAttribute},
 *     the call is fused into the field load via {@link BlockLoaderExpression}. The keyed sub-field's
 *     doc values are read directly instead of materializing the whole flattened JSON and re-parsing
 *     it per row. The path is the flat sub-field name as is (no parsing), so any path that passes
 *     verifier-time validation is eligible for pushdown.
 * </p>
 */
public class FieldExtract extends EsqlScalarFunction implements BlockLoaderExpression {
    private static final BytesRef TRUE_BYTES = new BytesRef("true");
    private static final BytesRef FALSE_BYTES = new BytesRef("false");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FieldExtract",
        FieldExtract::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FieldExtract.class)
        .binary(FieldExtract::new)
        .capabilities("returns_multi_value")
        .name("field_extract");

    private final Expression field;
    private final Expression path;

    @FunctionInfo(
        returnType = "keyword",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.5.0") },
        briefSummary = "Extracts a sub-field value from a flattened field as a keyword.",
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

            String values are returned without surrounding quotes, and numbers and booleans as their string
            representation. When the sub-field is multi-valued in the flattened field, the result is a multi-valued
            `keyword` block. Because the underlying mapper flattens nested objects into dotted keys at index time,
            a sub-field whose value is itself a JSON object has no leaf at the requested key in the flat storage and
            the function returns `null`. The dotted child paths (`a.b`, `a.b.c`, ...) still address the leaves directly.
            Inside a multi-value sub-field, JSON object elements are likewise absent from the flat storage and are
            skipped; nested JSON arrays are flattened recursively so all scalar leaves end up in the resulting
            multi-value block.""",
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
                    parser.nextToken(); // advance to the value token
                    if (name.equals(key)) {
                        appendValueAtCurrentToken(builder, parser, key);
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

    /**
     * Emits the JSON value at the parser's current token to {@code builder}. Mirrors the
     * keyed sub-field block-loader's per-cell shape so the parse-path output is interchangeable
     * with the pushdown path's output for the same source: scalars produce a single-value
     * position and JSON arrays produce a multi-value position with one keyword per scalar leaf.
     * A nested JSON object at the requested key produces a {@code null} position because the
     * flattened mapper indexes its leaves under dotted child keys ({@code key.a},
     * {@code key.a.b}, ...) and therefore stores no value at {@code key} itself; the same
     * dotted child paths still address the inner leaves directly. {@code VALUE_NULL} appends a
     * null position.
     */
    private static void appendValueAtCurrentToken(BytesRefBlock.Builder builder, XContentParser parser, String key) throws IOException {
        XContentParser.Token token = parser.currentToken();
        switch (token) {
            case VALUE_STRING, VALUE_NUMBER -> builder.appendBytesRef(new BytesRef(parser.text()));
            case VALUE_BOOLEAN -> builder.appendBytesRef(parser.booleanValue() ? TRUE_BYTES : FALSE_BYTES);
            case VALUE_NULL -> builder.appendNull();
            case START_OBJECT -> {
                parser.skipChildren();
                builder.appendNull();
            }
            case START_ARRAY -> appendArrayAsMultiValue(builder, parser, key);
            default -> throw new IllegalArgumentException("unexpected token [" + token + "] at path [" + key + "]");
        }
    }

    /**
     * Walks the current JSON array and emits the scalar leaves at the requested key as a
     * single block-builder position. Mirrors how the flattened mapper iterates leaf-array
     * elements without extending the storage key: nested arrays are walked recursively
     * because their scalar contents would also be indexed under the outer key, while
     * embedded objects are skipped because their leaves would be indexed under
     * <em>extended</em> dotted keys ({@code key.a}, {@code key.a.b}, ...) and never under
     * {@code key} itself. JSON null elements are dropped because a {@code BytesRefBlock}
     * multi-value position cannot represent {@code null} as one of its elements, and
     * dropping is the only representable option that does not silently substitute an empty
     * {@link BytesRef} for a missing value.
     * <p>
     * Buffers the rendered scalars first so the choice between scalar position, multi-value
     * position, and null can be made once the array is exhausted: an empty result (the
     * source array was empty, held only JSON nulls, or held only embedded objects whose
     * leaves live at extended keys) maps to a null position; a single-element result
     * collapses to a scalar position so the block is shaped identically to a doc whose
     * source already held a single value (symmetric to the single-element collapse
     * {@code CsvTestsDataLoader.parseDocument} performs on the way in); multi-element
     * results use a begin/end position-entry pair on the block builder.
     */
    private static void appendArrayAsMultiValue(BytesRefBlock.Builder builder, XContentParser parser, String key) throws IOException {
        List<BytesRef> elements = new ArrayList<>();
        collectScalarLeavesFromCurrentArray(parser, elements, key);
        if (elements.isEmpty()) {
            builder.appendNull();
            return;
        }
        if (elements.size() == 1) {
            builder.appendBytesRef(elements.get(0));
            return;
        }
        builder.beginPositionEntry();
        for (BytesRef e : elements) {
            builder.appendBytesRef(e);
        }
        builder.endPositionEntry();
    }

    /**
     * Consumes the parser's current array (up to and including the matching {@code END_ARRAY})
     * and appends every scalar leaf in document order to {@code elements}. Recurses into
     * nested arrays so their scalars are flattened into the same key (this matches how the
     * flattened mapper iterates leaf-array elements without extending the storage key).
     * Skips embedded JSON objects in full because their leaves live at extended dotted keys
     * ({@code key.a}, {@code key.a.b}, ...) and are never indexed at {@code key} itself.
     * Drops JSON nulls, as the caller's multi-value position cannot represent them.
     */
    private static void collectScalarLeavesFromCurrentArray(XContentParser parser, List<BytesRef> elements, String key) throws IOException {
        XContentParser.Token elem;
        while ((elem = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            switch (elem) {
                case VALUE_STRING, VALUE_NUMBER -> elements.add(new BytesRef(parser.text()));
                case VALUE_BOOLEAN -> elements.add(parser.booleanValue() ? TRUE_BYTES : FALSE_BYTES);
                case VALUE_NULL -> {
                    // see method-level Javadoc: drop nulls inside multi-value keyword positions.
                }
                case START_ARRAY -> collectScalarLeavesFromCurrentArray(parser, elements, key);
                case START_OBJECT -> parser.skipChildren();
                default -> throw new IllegalArgumentException("unexpected token [" + elem + "] inside array at path [" + key + "]");
            }
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

    /**
     * Whether {@code field_extract} is part of the active function set for this build, matching
     * the {@code fn_field_extract} entry emitted by {@link EsqlFunctionRegistry#addCapabilities}.
     * Block-loader fusion and Lucene query pushdown use the same gate so they stay aligned with
     * function registration (snapshot-only until the function is promoted to the main registry).
     */
    public static boolean isFnFieldExtractCapabilityMet() {
        return EsqlFunctionRegistry.isSnapshotOnly("field_extract") == false || Build.current().isSnapshot();
    }

    @Override
    public PushedBlockLoaderExpression tryPushToFieldLoading(SearchStats stats) {
        // Pushdown requires a real flattened field reference and a constant literal key. The
        // path argument is already the flat storage key (verifier rejects brackets and indices),
        // so we hand it to the keyed sub-field loader as is.
        if (EsqlCapabilities.Cap.FIELD_EXTRACT_FLATTENED_PUSHDOWN.isEnabled() == false) {
            return null;
        }
        return foldedKeyForFlattenedRoot().map(
            keyForRoot -> new PushedBlockLoaderExpression(keyForRoot.root(), new ExtractFlattenedSubfieldConfig(keyForRoot.key()))
        ).orElse(null);
    }

    /**
     * If this {@code field_extract} can be pushed to a Lucene query against the keyed sub-field,
     * returns the synthetic data-node field name (e.g. {@code "resource.attributes.host.name"}).
     * The data node's {@code FieldTypeLookup} resolves this name to a
     * {@code KeyedFlattenedFieldType} which handles the key-prefix encoding in
     * {@code indexedValueForSearch}. The caller is responsible for wrapping the produced query
     * in a {@code SingleValueQuery} to preserve ES|QL's single-value comparison semantics.
     * <p>
     *     Explicitly mapped sub-fields are <strong>not</strong> pushed: for them the synthetic name
     *     resolves to the real typed field (e.g. an {@code ip} or {@code long}), so a pushed query
     *     would apply that field's typed comparison semantics while the per-row evaluator compares the
     *     extracted value as a {@code keyword}. Keeping mapped sub-fields on the evaluator path makes
     *     {@code field_extract}'s result independent of whether the optimizer pushed the call. That
     *     rejection is delegated to the flattened field type's
     *     {@link org.elasticsearch.index.mapper.MappedFieldType#supportsBlockLoaderConfig}, the same hook that
     *     gates block-loader fusion, so both pushdown paths agree on which keys are pushable. The
     *     mapped/unmapped decision needs the data-node mapping, so the stats-less {@code can_match} predicates
     *     report no loader config as supported and push nothing.
     * </p>
     */
    public Optional<String> tryAsKeyedSubfieldName(LucenePushdownPredicates pushdownPredicates) {
        if (EsqlCapabilities.Cap.FIELD_EXTRACT_FLATTENED_PUSHDOWN.isEnabled() == false) {
            return Optional.empty();
        }
        return foldedKeyForFlattenedRoot().filter(k -> pushdownPredicates.isIndexedAndHasDocValues(k.root()))
            .filter(
                k -> pushdownPredicates.supportsLoaderConfig(
                    k.root(),
                    new ExtractFlattenedSubfieldConfig(k.key()),
                    MappedFieldType.FieldExtractPreference.NONE
                )
            )
            .map(k -> k.root().name() + "." + k.key());
    }

    /**
     * Common precondition check for both block-loader and query pushdown: the field must be a real
     * {@link FieldAttribute} of {@link DataType#FLATTENED} type, and the path must fold to a literal
     * flat key. {@link #resolveType} has already validated the key shape via
     * {@link #validateFieldExtractPath} for any foldable path, so by the time we reach pushdown the
     * key is guaranteed to be a non-empty literal sub-field name (no brackets, no array indices).
     */
    private Optional<RootAndKey> foldedKeyForFlattenedRoot() {
        if (field instanceof FieldAttribute fa
            && fa.dataType() == DataType.FLATTENED
            && path.foldable()
            && path.fold(FoldContext.small()) instanceof BytesRef foldedPath) {
            return Optional.of(new RootAndKey(fa, foldedPath.utf8ToString()));
        }
        return Optional.empty();
    }

    private record RootAndKey(FieldAttribute root, String key) {}
}
