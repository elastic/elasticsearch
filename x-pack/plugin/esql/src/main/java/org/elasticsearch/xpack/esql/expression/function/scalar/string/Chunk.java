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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Chunk extends EsqlScalarFunction implements TwoOptionalArguments {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Chunk", Chunk::new);

    public static final int DEFAULT_NUM_CHUNKS = Integer.MAX_VALUE;
    public static final int DEFAULT_CHUNK_SIZE = 300;

    private final Expression field, numChunks, chunkSize;

    @FunctionInfo(returnType = "keyword", preview = true, description = """
        Use `CHUNK` to split a text field into smaller chunks.""", detailedDescription = """
            Chunk can be used on fields from the text famiy like <<text, text>> and <<semantic-text, semantic_text>>.
            Chunk will split a text field into smaller chunks, using a sentence-based chunking strategy.
            The number of chunks returned, and the length of the sentences used to create the chunks can be specified.
        """, examples = { @Example(file = "chunk", tag = "chunk-with-field", applies_to = "stack: preview 9.3.0") })
    public Chunk(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "The input to chunk.") Expression field,
        @Param(
            optional = true,
            name = "num_chunks",
            type = { "integer" },
            description = "The number of chunks to return. Defaults to return all chunks."
        ) Expression numChunks,
        @Param(
            optional = true,
            name = "chunk_size",
            type = { "integer" },
            description = "The size of sentence-based chunks to use. Defaults to " + DEFAULT_CHUNK_SIZE
        ) Expression chunkSize
    ) {
        super(source, fields(field, numChunks, chunkSize));
        this.field = field;
        this.numChunks = numChunks;
        this.chunkSize = chunkSize;
    }

    public Chunk(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeOptionalNamedWriteable(numChunks);
        out.writeOptionalNamedWriteable(chunkSize);
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

        return isString(field(), sourceText(), FIRST);
    }

    @Override
    public boolean foldable() {
        return field().foldable() && (numChunks() == null || numChunks().foldable()) && (chunkSize() == null || chunkSize().foldable());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Chunk(
            source(),
            newChildren.get(0), // field
            numChunks == null ? null : newChildren.get(1),
            chunkSize == null ? null : newChildren.get(2)
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Chunk::new, field, numChunks, chunkSize);
    }

    Expression field() {
        return field;
    }

    Expression numChunks() {
        return numChunks;
    }

    Expression chunkSize() {
        return chunkSize;
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, BytesRef str, int numChunks, int chunkSize) {
        if (numChunks < 0) {
            throw new IllegalArgumentException("Num chunks parameter cannot be negative, found [" + numChunks + "]");
        }
        if (chunkSize < 0) {
            throw new IllegalArgumentException("Chunk size parameter cannot be negative, found [" + chunkSize + "]");
        }

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
            .limit(numChunks > 0 ? numChunks : Long.MAX_VALUE)
            .toList();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Chunk chunk = (Chunk) o;
        return Objects.equals(field(), chunk.field())
            && Objects.equals(numChunks(), chunk.numChunks())
            && Objects.equals(chunkSize(), chunk.chunkSize());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), numChunks(), chunkSize());
    }

    private static List<Expression> fields(Expression field, Expression numChunks, Expression chunkSize) {
        List<Expression> list = new ArrayList<>(4);
        list.add(field);
        if (numChunks != null) {
            list.add(numChunks);
        }
        if (chunkSize != null) {
            list.add(chunkSize);
        }
        return list;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new ChunkBytesRefEvaluator.Factory(
            source(),
            toEvaluator.apply(field),
            numChunks != null
                ? toEvaluator.apply(numChunks)
                : toEvaluator.apply(new Literal(source(), DEFAULT_NUM_CHUNKS, DataType.INTEGER)),
            chunkSize != null
                ? toEvaluator.apply(chunkSize)
                : toEvaluator.apply(new Literal(source(), DEFAULT_CHUNK_SIZE, DataType.INTEGER))
        );
    }
}
