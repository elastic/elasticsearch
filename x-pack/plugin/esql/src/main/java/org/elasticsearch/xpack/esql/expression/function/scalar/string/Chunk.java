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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.inference.chunking.Chunker;
import org.elasticsearch.xpack.core.inference.chunking.ChunkerBuilder;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
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
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Chunk extends EsqlScalarFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Chunk", Chunk::new);

    public static final int DEFAULT_NUM_CHUNKS = Integer.MAX_VALUE;
    public static final int DEFAULT_CHUNK_SIZE = 300;

    private final Expression field, options;

    static final String NUM_CHUNKS = "num_chunks";
    static final String CHUNK_SIZE = "chunk_size";

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.of(NUM_CHUNKS, DataType.INTEGER, CHUNK_SIZE, DataType.INTEGER);

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
                    name = "chunk_size",
                    type = "integer",
                    description = "The size of sentence-based chunks to use. Defaults to " + DEFAULT_CHUNK_SIZE
                ), },
            description = "Options to customize chunking behavior.",
            optional = true
        ) Expression options
    ) {
        super(source, options == null ? List.of(field) : List.of(field, options));
        this.field = field;
        this.options = options;
    }

    private Chunk(
        Source source,
        Expression field,
        Expression options,
        boolean unused // dummy parameter to differentiate constructors
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
        return isString(field(), sourceText(), FIRST).and(Options.resolve(options, source(), SECOND, ALLOWED_OPTIONS, this::verifyOptions));
    }

    private void verifyOptions(Map<String, Object> optionsMap) {
        if (options == null) {
            return;
        }

        Integer numChunks = (Integer) optionsMap.get(NUM_CHUNKS);
        if (numChunks != null && numChunks < 0) {
            throw new InvalidArgumentException("[{}] cannot be negative, found [{}]", NUM_CHUNKS, numChunks);
        }
        Integer chunkSize = (Integer) optionsMap.get(CHUNK_SIZE);
        if (chunkSize != null && chunkSize < 0) {
            throw new InvalidArgumentException("[{}] cannot be negative, found [{}]", CHUNK_SIZE, chunkSize);
        }

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
    static void process(BytesRefBlock.Builder builder, BytesRef str, int numChunks, int chunkSize) {
        String content = str.utf8ToString();

        ChunkingSettings settings = new SentenceBoundaryChunkingSettings(chunkSize, 0);
        List<String> chunks = chunkText(content, settings, numChunks);

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

        Map<String, Object> optionsMap = new HashMap<>();
        if (options() != null) {
            Options.populateMap(((MapExpression) options), optionsMap, source(), SECOND, ALLOWED_OPTIONS);
        }

        int numChunks = (Integer) optionsMap.getOrDefault(NUM_CHUNKS, DEFAULT_NUM_CHUNKS);
        int chunkSize = (Integer) optionsMap.getOrDefault(CHUNK_SIZE, DEFAULT_CHUNK_SIZE);

        return new ChunkBytesRefEvaluator.Factory(
            source(),
            toEvaluator.apply(field),
            toEvaluator.apply(new Literal(source(), numChunks, DataType.INTEGER)),
            toEvaluator.apply(new Literal(source(), chunkSize, DataType.INTEGER))
        );
    }
}
