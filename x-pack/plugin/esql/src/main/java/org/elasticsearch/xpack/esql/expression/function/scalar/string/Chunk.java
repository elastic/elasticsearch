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
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.inference.chunking.Chunker;
import org.elasticsearch.xpack.core.inference.chunking.ChunkerBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsOptions;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
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
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Chunk extends EsqlScalarFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Chunk", Chunk::new);

    static final int DEFAULT_CHUNK_SIZE = 300;
    public static final ChunkingSettings DEFAULT_CHUNKING_SETTINGS = new SentenceBoundaryChunkingSettings(DEFAULT_CHUNK_SIZE, 0);

    private final Expression field, chunkingSettings;

    public static final Map<String,DataType> ALLOWED_CHUNKING_SETTING_OPTIONS = Map.ofEntries(
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
        examples = {
            @Example(file = "chunk", tag = "chunk-with-field", applies_to = "stack: preview 9.3.0"),
            @Example(file = "chunk", tag = "chunk-with-chunking-settings", applies_to = "stack: preview 9.3.0") }
    )
    public Chunk(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "The input to chunk.") Expression field,
        @MapParam(
            name = "chunking_settings",
            description = "Options to customize chunking behavior. Refer to the "
                + "[Inference API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put"
                + "#operation-inference-put-body-application-json-chunking_settings) for valid values for `chunking_settings`.",
            optional = true
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

        return isString(field(), sourceText(), FIRST).and(Options.resolve(chunkingSettings, source(), SECOND,
            ALLOWED_CHUNKING_SETTING_OPTIONS, this::validateChunkingSettings));
    }

    private void validateChunkingSettings(Map<String,Object> chunkingSettingsMap) {
        if (chunkingSettings == null) {
            return;
        }

        try {
            toChunkingSettings(chunkingSettingsMap);
        } catch (IllegalArgumentException e) {
            throw new InvalidArgumentException(e.getMessage(), e);
        }
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

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, BytesRef str, @Fixed ChunkingSettings chunkingSettings) {
        String content = str.utf8ToString();

        List<String> chunks = chunkText(content, chunkingSettings);

        boolean multivalued = chunks.size() > 1;
        if (multivalued) {
            builder.beginPositionEntry();
        }
        for (String chunk : chunks) {
            builder.appendBytesRef(new BytesRef(chunk.trim()));
        }

        if (multivalued) {
            builder.endPositionEntry();
        }
    }

    public static List<String> chunkText(String content, ChunkingSettings chunkingSettings) {
        Chunker chunker = ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy());
        return chunker.chunk(content, chunkingSettings).stream().map(offset -> content.substring(offset.start(), offset.end())).toList();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Chunk chunk = (Chunk) o;
        return Objects.equals(field(), chunk.field()) && Objects.equals(chunkingSettings(), chunk.chunkingSettings());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), chunkingSettings());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ChunkingSettings chunkingSettings = DEFAULT_CHUNKING_SETTINGS;

        if (chunkingSettings() != null) {
            chunkingSettings = toChunkingSettings((MapExpression) chunkingSettings());
        }

        return new ChunkBytesRefEvaluator.Factory(source(), toEvaluator.apply(field), chunkingSettings);
    }

    // TODO remove?
    private static ChunkingSettings toChunkingSettings(MapExpression map) {
        Map<String, Object> chunkingSettingsMap = map.keyFoldedMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            Object value = e.getValue().fold(FoldContext.small());
            return value instanceof BytesRef ? ((BytesRef) value).utf8ToString() : value;
        }));
        return ChunkingSettingsBuilder.fromMap(chunkingSettingsMap);
    }

    private static ChunkingSettings toChunkingSettings(Map<String,Object> expressionMap) {
        Map<String,Object> chunkingSettingsMap = expressionMap.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                Object value = e.getValue();
                return value instanceof BytesRef ? ((BytesRef) value).utf8ToString() : value;
            }));
        return ChunkingSettingsBuilder.fromMap(chunkingSettingsMap);
    }
}
