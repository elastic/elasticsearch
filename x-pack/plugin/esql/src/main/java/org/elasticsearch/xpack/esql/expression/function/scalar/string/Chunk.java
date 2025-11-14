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
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Chunk extends EsqlScalarFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Chunk", Chunk::new);

    public static final int DEFAULT_NUM_CHUNKS = Integer.MAX_VALUE;
    static final int DEFAULT_CHUNK_SIZE = 300;
    public static final ChunkingSettings DEFAULT_CHUNKING_SETTINGS = new SentenceBoundaryChunkingSettings(DEFAULT_CHUNK_SIZE, 0);

    private final Expression field, options;

    static final String NUM_CHUNKS = "num_chunks";
    static final String CHUNKING_SETTINGS = "chunking_settings";

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.of(NUM_CHUNKS, DataType.INTEGER, CHUNKING_SETTINGS, DataType.OBJECT);

    @FunctionInfo(returnType = "keyword", preview = true, description = """
        Use `CHUNK` to split a text field into smaller chunks.""", detailedDescription = """
            Chunk can be used on fields from the text famiy like <<text, text>> and <<semantic-text, semantic_text>>.
            Chunk will split a text field into smaller chunks, using a sentence-based chunking strategy.
            The number of chunks returned, and the length of the sentences used to create the chunks can be specified.
        """, examples = { @Example(file = "chunk", tag = "chunk-with-field", applies_to = "stack: preview 9.3.0") })
    public Chunk(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "The input to chunk.") Expression field,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "num_chunks",
                    type = "integer",
                    description = "The number of chunks to return. Defaults to return all chunks."
                ),
                @MapParam.MapParamEntry(
                    name = "chunking_settings",
                    type = "object",
                    description = "The chunking settings with which to apply to the field. " +
                        "If no chunking settings are specified, defaults to sentence-based chunks of size " + DEFAULT_CHUNK_SIZE
                ), },
            description = "Options to customize chunking behavior.",
            optional = true
        ) Expression options
    ) {
        super(source, options == null ? List.of(field) : List.of(field, options));
        this.field = field;
        this.options = options;
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
        out.writeOptionalNamedWriteable(options);
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

        TypeResolution fieldResolution = isString(field(), sourceText(), FIRST);
        if (fieldResolution.unresolved()) {
            return fieldResolution;
        }

        return options == null ? TypeResolution.TYPE_RESOLVED : validateOptions();
    }

    private TypeResolution validateOptions() {
        // TODO - Options#resolve should play nicely with nested MapExpressions, doing a hacky manual evaluation for now
        if (options instanceof MapExpression == false) {
            return new TypeResolution("second argument of [" + sourceText() + "] must be a map");
        }

        MapExpression mapExpr = (MapExpression) options;
        for (EntryExpression entry : mapExpr.entryExpressions()) {
            if (entry.key() instanceof Literal == false || entry.key().foldable() == false) {
                return new TypeResolution("option names must be constants in [" + sourceText() + "]");
            }

            Object keyValue = ((Literal) entry.key()).value();
            String optionName = keyValue instanceof BytesRef br ? br.utf8ToString() : keyValue.toString();

            if (NUM_CHUNKS.equals(optionName)) {
                if (entry.value() instanceof Literal == false) {
                    return new TypeResolution("[" + NUM_CHUNKS + "] must be a constant");
                }
                Literal value = (Literal) entry.value();
                if (value.dataType() != DataType.INTEGER) {
                    return new TypeResolution("[" + NUM_CHUNKS + "] must be an integer, found [" + value.dataType() + "]");
                }
                Integer numChunks = (Integer) value.value();
                if (numChunks != null && numChunks < 0) {
                    return new TypeResolution("[" + NUM_CHUNKS + "] cannot be negative, found [" + numChunks + "]");
                }
            } else if (CHUNKING_SETTINGS.equals(optionName)) {
                if (entry.value() instanceof MapExpression == false) {
                    return new TypeResolution(
                        "[" + CHUNKING_SETTINGS + "] must be a map, found [" + entry.value().dataType() + "]");
                }
                return validateChunkingSettings(entry.value());
            } else {
                return new TypeResolution(
                    "Invalid option [" + optionName + "], expected one of [" + String.join(", ", ALLOWED_OPTIONS.keySet()) + "]");
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    private TypeResolution validateChunkingSettings(Expression chunkingSettings) {
        // Just ensure all keys and values are literals - defer valid chunking settings for validation later
        assert chunkingSettings instanceof MapExpression;
        MapExpression chunkingSettingsMap = (MapExpression) chunkingSettings;
        for (EntryExpression entry : chunkingSettingsMap.entryExpressions()) {
            if (entry.key() instanceof Literal == false || (entry.key()).foldable() == false) {
                return new TypeResolution("chunking_settings keys must be constants");
            }
            if (entry.value() instanceof Literal == false || (entry.value()).foldable() == false) {
                return new TypeResolution("chunking_settings values must be constants");
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return field().foldable() && (options() == null || options().foldable());
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
        return NodeInfo.create(this, Chunk::new, field, options);
    }

    Expression field() {
        return field;
    }

    Expression options() {
        return options;
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, BytesRef str, int numChunks, @Fixed ChunkingSettings chunkingSettings) {
        String content = str.utf8ToString();

        List<String> chunks = chunkText(content, chunkingSettings, numChunks);

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

    public static List<String> chunkText(String content, ChunkingSettings chunkingSettings, int numChunks) {
        Chunker chunker = ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy());
        return chunker.chunk(content, chunkingSettings)
            .stream()
            .map(offset -> content.substring(offset.start(), offset.end()))
            .limit(numChunks > 0 ? numChunks : DEFAULT_NUM_CHUNKS)
            .toList();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Chunk chunk = (Chunk) o;
        return Objects.equals(field(), chunk.field()) && Objects.equals(options(), chunk.options());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), options());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        int numChunks = DEFAULT_NUM_CHUNKS;
        ChunkingSettings chunkingSettings = DEFAULT_CHUNKING_SETTINGS;

        if (options() != null) {
            MapExpression mapExpr = (MapExpression) options();
            for (EntryExpression entry : mapExpr.entryExpressions()) {
                Object keyValue = ((Literal) entry.key()).value();
                String optionName = keyValue instanceof BytesRef br ? br.utf8ToString() : keyValue.toString();

                if (NUM_CHUNKS.equals(optionName)) {
                    numChunks = (Integer) ((Literal) entry.value()).value();
                } else if (CHUNKING_SETTINGS.equals(optionName)) {
                    // Convert the nested MapExpression to Map<String, Object> and build ChunkingSettings
                    Map<String, Object> chunkingSettingsMap = toMap((MapExpression) entry.value());
                    chunkingSettings = ChunkingSettingsBuilder.fromMap(chunkingSettingsMap);
                }
            }
        }

        return new ChunkBytesRefEvaluator.Factory(
            source(),
            toEvaluator.apply(field),
            toEvaluator.apply(new Literal(source(), numChunks, DataType.INTEGER)),
            chunkingSettings
        );
    }

    private static Map<String, Object> toMap(MapExpression mapExpr) {
        Map<String, Object> result = new java.util.HashMap<>();
        for (EntryExpression entry : mapExpr.entryExpressions()) {
            Object keyValue = ((Literal) entry.key()).value();
            String key = keyValue instanceof BytesRef br ? br.utf8ToString() : keyValue.toString();

            Object value = ((Literal) entry.value()).value();
            if (value instanceof BytesRef br) {
                value = br.utf8ToString();
            }
            result.put(key, value);
        }
        return result;
    }
}
