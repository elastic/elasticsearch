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
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsOptions;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.emitChunks;

public class Chunk extends EsqlScalarFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Chunk", Chunk::new);

    static final int DEFAULT_CHUNK_SIZE = 300;
    public static final ChunkingSettings DEFAULT_CHUNKING_SETTINGS = new SentenceBoundaryChunkingSettings(DEFAULT_CHUNK_SIZE, 0);

    private final Expression field, chunkingSettings;

    public static final Map<String, DataType> ALLOWED_CHUNKING_SETTING_OPTIONS = Map.ofEntries(
        entry(ChunkingSettingsOptions.STRATEGY.toString(), DataType.KEYWORD),
        entry(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), DataType.INTEGER),
        entry(ChunkingSettingsOptions.OVERLAP.toString(), DataType.INTEGER),
        entry(ChunkingSettingsOptions.SENTENCE_OVERLAP.toString(), DataType.INTEGER),
        entry(ChunkingSettingsOptions.SEPARATOR_GROUP.toString(), DataType.KEYWORD),
        entry(ChunkingSettingsOptions.SEPARATORS.toString(), DataType.KEYWORD)
    );

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        returnType = "keyword",
        preview = true,
        description = """
            Use `CHUNK` to split a text field into smaller chunks.""",
        detailedDescription = """
                Chunk can be used on fields from the text famiy like <<text, text>> and <<semantic-text, semantic_text>>.
                Chunk will split a text field into smaller chunks, using a sentence-based chunking strategy.
                The number of chunks returned, and the length of the sentences used to create the chunks can be specified.
            """,
        examples = { @Example(file = "chunk", tag = "chunk-example", applies_to = "stack: preview 9.3.0") }
    )
    public Chunk(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text" },
            description = "The input to chunk. The input can be a single-valued or multi-valued field. In the case of a multi-valued "
                + "argument, each value is chunked separately."
        ) Expression field,
        @MapParam(
            name = "chunking_settings",
            description = "Options to customize chunking behavior. Defaults to "
                + "{\"strategy\":\"sentence\",\"max_chunk_size\":"
                + DEFAULT_CHUNK_SIZE
                + ",\"sentence_overlap\":0}.",
            optional = true,
            params = {
                @MapParam.MapParamEntry(
                    name = "strategy",
                    type = { "keyword" },
                    description = "The chunking strategy to use. Default value is `sentence`.",
                    valueHint = { "sentence", "word", "none", "recursive" }
                ),
                @MapParam.MapParamEntry(name = "max_chunk_size", type = { "integer" }, description = """
                    The maximum size of a chunk in words. This value cannot be lower than `20` (for `sentence` strategy)
                    or `10` (for `word` or `recursive` strategies). This model should not exceed the window size for any
                    associated models using the output of this function.
                    """, valueHint = { "300" }),
                @MapParam.MapParamEntry(name = "overlap", type = { "integer" }, description = """
                    The number of overlapping words for chunks. It is applicable only to a `word` chunking strategy.
                    This value cannot be higher than half the `max_chunk_size` value.
                    """, valueHint = { "0" }),
                @MapParam.MapParamEntry(name = "sentence_overlap", type = { "integer" }, description = """
                    The number of overlapping sentences for chunks. It is applicable only for a `sentence` chunking strategy.
                    It can be either `1` or `0`.
                    """, valueHint = { "1", "0" }),
                @MapParam.MapParamEntry(name = "separator_group", type = { "keyword" }, description = """
                    Sets a predefined lists of separators based on the selected text type. Values may be `markdown` or `plaintext`.
                    Only applicable to the `recursive` chunking strategy. When using the `recursive` chunking strategy one of
                    `separators` or `separator_group` must be specified.
                    """, valueHint = { "markdown", "plaintext" }),
                @MapParam.MapParamEntry(name = "separators", type = { "keyword" }, description = """
                    A list of strings used as possible split points when chunking text. Each string can be a plain string or a
                    regular expression (regex) pattern. The system tries each separator in order to split the text, starting from
                    the first item in the list. After splitting, it attempts to recombine smaller pieces into larger chunks that stay
                    within the `max_chunk_size` limit, to reduce the total number of chunks generated. Only applicable to the
                    `recursive` chunking strategy. When using the `recursive` chunking strategy one of `separators` or `separator_group`
                    must be specified.
                    """, valueHint = { "(?<!\\n)\\n\\n(?!\\n)", "(?<!\\n)\\n(?!\\n)" }) }
        ) Expression chunkingSettings
    ) {
        super(source, chunkingSettings == null ? List.of(field) : List.of(field, chunkingSettings));
        this.field = field;
        this.chunkingSettings = chunkingSettings;
    }

    public Chunk(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeOptionalNamedWriteable(chunkingSettings);
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

        return isString(field(), sourceText(), FIRST).and(this::validateChunkingSettings);
    }

    private TypeResolution validateChunkingSettings() {
        if (chunkingSettings == null) {
            return TypeResolution.TYPE_RESOLVED;
        }
        if (chunkingSettings instanceof MapExpression == false) {
            return new TypeResolution("invalid chunking_settings, found [" + chunkingSettings.sourceText() + "]");
        }
        MapExpression chunkingSettingsMap = (MapExpression) chunkingSettings;
        var errors = chunkingSettingsMap.keyFoldedMap()
            .entrySet()
            .stream()
            .filter(e -> e.getValue() instanceof Literal == false)
            .map(e -> "invalid option for [" + e.getKey() + "], expected a constant, found [" + e.getValue().dataType() + "]")
            .toList();

        if (errors.isEmpty() == false) {
            return new TypeResolution(String.join("; ", errors));
        }

        try {
            toChunkingSettings(chunkingSettingsMap);
        } catch (IllegalArgumentException e) {
            return new TypeResolution(e.getMessage());
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return field().foldable() && (chunkingSettings() == null || chunkingSettings().foldable());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Chunk(
            source(),
            newChildren.get(0), // field
            newChildren.size() > 1 ? newChildren.get(1) : null // options
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Chunk::new, field, chunkingSettings);
    }

    Expression field() {
        return field;
    }

    Expression chunkingSettings() {
        return chunkingSettings;
    }

    @Evaluator
    static void process(
        BytesRefBlock.Builder builder,
        @Position int position,
        BytesRefBlock field,
        @Fixed ChunkingSettings chunkingSettings
    ) {
        int valueCount = field.getValueCount(position);
        if (valueCount == 0) {
            builder.appendNull();
            return;
        }

        int firstValueIndex = field.getFirstValueIndex(position);

        // Collect all chunks from all values in the multi-value field
        List<String> allChunks = new java.util.ArrayList<>();
        for (int i = 0; i < valueCount; i++) {
            BytesRef value = field.getBytesRef(firstValueIndex + i, new BytesRef());
            String content = value.utf8ToString();
            List<String> chunks = chunkText(content, chunkingSettings);
            allChunks.addAll(chunks);
        }

        // Emit all chunks combined
        emitChunks(builder, allChunks);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ChunkingSettings chunkingSettings = DEFAULT_CHUNKING_SETTINGS;

        if (chunkingSettings() != null) {
            chunkingSettings = toChunkingSettings((MapExpression) chunkingSettings());
        }

        var fieldEvaluator = toEvaluator.apply(field);
        return new ChunkEvaluator.Factory(source(), fieldEvaluator, chunkingSettings);
    }

    private static ChunkingSettings toChunkingSettings(MapExpression map) {
        Map<String, Object> chunkingSettingsMap = map.keyFoldedMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            Object value = e.getValue().fold(FoldContext.small());
            if (value instanceof BytesRef bytesRef) {
                return bytesRef.utf8ToString();
            } else if (value instanceof List<?> list) {
                return list.stream().map(item -> item instanceof BytesRef ? ((BytesRef) item).utf8ToString() : item).toList();
            }
            return value;
        }));
        return ChunkingSettingsBuilder.fromMap(chunkingSettingsMap);
    }
}
