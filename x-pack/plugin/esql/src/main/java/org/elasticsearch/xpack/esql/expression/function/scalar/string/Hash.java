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
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Hash extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Hash", Hash::new);

    private final Expression algorithm;
    private final Expression input;

    @FunctionInfo(
        returnType = "keyword",
        description = "Computes the hash of the input using various algorithms such as MD5, SHA, SHA-224, SHA-256, SHA-384, SHA-512.",
        examples = { @Example(file = "hash", tag = "hash") }
    )
    public Hash(
        Source source,
        @Param(name = "algorithm", type = { "keyword", "text" }, description = "Hash algorithm to use.") Expression algorithm,
        @Param(name = "input", type = { "keyword", "text" }, description = "Input to hash.") Expression input
    ) {
        super(source, List.of(algorithm, input));
        this.algorithm = algorithm;
        this.input = input;
    }

    private Hash(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(algorithm);
        out.writeNamedWriteable(input);
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

        TypeResolution resolution = isString(algorithm, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isString(input, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return algorithm.foldable() && input.foldable();
    }

    @Evaluator(warnExceptions = NoSuchAlgorithmException.class)
    static BytesRef process(
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch,
        BytesRef algorithm,
        BytesRef input
    ) throws NoSuchAlgorithmException {
        return hash(scratch, MessageDigest.getInstance(algorithm.utf8ToString()), input);
    }

    @Evaluator(extraName = "Constant")
    static BytesRef processConstant(
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch,
        @Fixed(scope = THREAD_LOCAL) HashFunction algorithm,
        BytesRef input
    ) {
        return hash(scratch, algorithm.digest, input);
    }

    private static BytesRef hash(BreakingBytesRefBuilder scratch, MessageDigest algorithm, BytesRef input) {
        algorithm.reset();
        algorithm.update(input.bytes, input.offset, input.length);
        var digest = algorithm.digest();
        scratch.clear();
        scratch.grow(digest.length * 2);
        appendUtf8HexDigest(scratch, digest);
        return scratch.bytesRefView();
    }

    private static final byte[] ASCII_HEX_BYTES = new byte[] { 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 97, 98, 99, 100, 101, 102 };

    /**
     * This function allows to append hex bytes dirrectly to the {@link BreakingBytesRefBuilder}
     * bypassing unnecessary array allocations and byte array copying.
     */
    private static void appendUtf8HexDigest(BreakingBytesRefBuilder scratch, byte[] bytes) {
        for (byte b : bytes) {
            scratch.append(ASCII_HEX_BYTES[b >> 4 & 0xf]);
            scratch.append(ASCII_HEX_BYTES[b & 0xf]);
        }
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (algorithm.foldable()) {
            try {
                // hash function is created here in order to validate the algorithm is valid before evaluator is created
                var hf = HashFunction.create((BytesRef) algorithm.fold(toEvaluator.foldCtx()));
                return new HashConstantEvaluator.Factory(
                    source(),
                    context -> new BreakingBytesRefBuilder(context.breaker(), "hash"),
                    new Function<>() {
                        @Override
                        public HashFunction apply(DriverContext context) {
                            return hf.copy();
                        }

                        @Override
                        public String toString() {
                            return hf.toString();
                        }
                    },
                    toEvaluator.apply(input)
                );
            } catch (NoSuchAlgorithmException e) {
                throw new InvalidArgumentException(e, "invalid algorithm for [{}]: {}", sourceText(), e.getMessage());
            }
        } else {
            return new HashEvaluator.Factory(
                source(),
                context -> new BreakingBytesRefBuilder(context.breaker(), "hash"),
                toEvaluator.apply(algorithm),
                toEvaluator.apply(input)
            );
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Hash(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Hash::new, children().get(0), children().get(1));
    }

    public record HashFunction(String algorithm, MessageDigest digest) {

        public static HashFunction create(String algorithm) {
            try {
                return new HashFunction(algorithm, MessageDigest.getInstance(algorithm));
            } catch (NoSuchAlgorithmException e) {
                assert false : "Expected to create a valid hashing algorithm";
                throw new IllegalStateException(e);
            }
        }

        public static HashFunction create(BytesRef literal) throws NoSuchAlgorithmException {
            var algorithm = literal.utf8ToString();
            return new HashFunction(algorithm, MessageDigest.getInstance(algorithm));
        }

        public HashFunction copy() {
            try {
                return new HashFunction(algorithm, MessageDigest.getInstance(algorithm));
            } catch (NoSuchAlgorithmException e) {
                assert false : "Algorithm should be valid at this point";
                throw new IllegalStateException(e);
            }
        }

        @Override
        public String toString() {
            return algorithm;
        }
    }

    Expression algorithm() {
        return algorithm;
    }

    Expression input() {
        return input;
    }
}
