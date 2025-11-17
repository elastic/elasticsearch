/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneCountOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;

import java.util.List;

/**
 * A {@code ScalarFunction} is a {@code Function} that makes one output value per
 * input row. It operates on a whole {@link Page} of inputs at a time, building
 * a {@link Block} of results.
 * <p>
 *     You see them in the language everywhere:
 * </p>
 * <ul>
 *     <li>{@code | EVAL foo_msg = CONCAT("foo ", message)}</li>
 *     <li>{@code | EVAL foo_msg = a + b}</li>
 *     <li>{@code | WHERE STARTS_WITH(a, "rabbit")}</li>
 *     <li>{@code | WHERE a == b}</li>
 *     <li>{@code | STATS AGG BY ----> a + b <---- this is a scalar}</li>
 *     <li>{@code | STATS AGG(----> a + b <---- this is a scalar)}</li>
 * </ul>
 * <p>
 *     Let's work the example of {@code CONCAT("foo ", message)}. It's called with a Page
 *     of inputs and resolves both of its parameters, yielding a constant block containing
 *     "foo " and a Block of strings containing {@code message}. It can expect to receive
 *     thousands of {@code message} values in that block. Then it builds and returns the block
 *     {@code "foo <message>"}.
 * </p>
 * <pre>{@code
 *   foo | message | result
 *   --- | ------- | ----------
 *   foo | bar     | foo bar
 *   foo | longer  | foo longer
 *   ... a thousand rows ...
 *   foo | baz     | foo baz
 * }</pre>
 * <p>
 *     It does this once per input Page.
 * </p>
 * <p>
 *     We have a guide for writing these in the javadoc for
 *     {@link org.elasticsearch.xpack.esql.expression.function.scalar}.
 * </p>
 * <h2>Optimizations</h2>
 * <p>
 *     Scalars are a <strong>huge</strong> part of the language, and we have a ton of
 *     different classes of optimizations for them that exist on a performance spectrum
 * </p>
 * <pre>{@code
 *  Better         Load Less and
 * than O(rows)     Run Faster               Run Faster                 Block-at-a-time     Tuple-at-a-time
 *     |----------------|-------------------------|------------------------------|-------------------|
 *     ^  ^  ^     ^    ^      ^                              ^    ^   ^   ^     ^      ^            ^
 *    CF LT ET    FP   BL     MBL                            SE  SIMD NO  CE   EVAL    EE          CASE
 * }</pre>
 * <ul>
 *     <li>
 *         {@code CF}: The fastest way to run a scalar, now and forever, is to run it at compile time,
 *         turn it into a constant, and propagate this throughout the query. This is called
 *         "constant folding" and all scalars, when their arguments are constants, are "folded"
 *         to a constant. Those
 *     </li>
 *     <li>
 *         {@code LT}: Run the TopN operation in Lucene. Fundamentally, lucene is a tuple-at-a-time
 *         engine that flows the <a href="https://www.elastic.co/blog/faster-retrieval-of-top-hits-in-elasticsearch-with-block-max-wand">min-competitive</a>
 *         sort key back into the index iteration process, allowing it to skip huge swaths
 *         of documents. It has quite a few optimizations that soften the blow of it being
 *         tuple-at-a-time, so these days "push to a lucene topn" is the fastest way you
 *         are going to run a scalar function. For that to work it has to be a {@code SORT}
 *         key and all the filters have to be pushable to lucene and lucene has to know
 *         how to run the function natively.
 *     </li>
 *     <li>
 *         {@code ET}: (<strong>HYPOTHETICAL</strong>) If ESQL's {@link TopNOperator} knew
 *         how to expose min-competitive information, and we fed it back into the lucene
 *         query operators then we too could do better than {@code O(matching_rows)} for
 *         queries sorting on the results of a scalar. This is like the {@code LT} but
 *         without as many limitations.
 *         See <a href="https://github.com/elastic/elasticsearch/issues/136267">issue</a>.
 *     </li>
 *     <li>
 *         {@code FP}: Some functions can be translated into a sequence of filters and pushed
 *         to lucene. If you are <strong>just</strong> counting documents then this can use
 *         the {@link LuceneCountOperator} which can count the number of matching documents
 *         directly from the cache, <strong>technically</strong> being faster than
 *         {@code O(num_hits)}, but only in ideal circumstances. If we can't push the count
 *         then it's still very very fast. See <a href="https://github.com/elastic/elasticsearch/pull/138023">PR</a>.
 *     </li>
 *     <li>
 *         {@code BL}: Some functions can take advantage of the on-disk structures to run
 *         very fast and should be "fused" into field loading using {@link BlockLoaderExpression}.
 *         This is also helpful for functions that throw out a bunch of data, especially if they
 *         can use the on disk storage to do so efficiently ({@code MV_MIN}) or if the data being
 *         loaded is super, ultra big ({@code ST_SIMPLIFY}). All of the {@link VectorSimilarityFunction}s
 *         are <strong>wonderful</strong> candidates for this because vectors are large, and they
 *         can use the search index to avoid loading the vectors into memory at all. This requires
 *         a {@link BlockLoader} for each {@code FUNCTION x FIELD_TYPE x storage mechanism}
 *     </li>
 *     <li>
 *         {@code MBL}: (<strong>HYPOTHETICAL</strong>) If we want to fuse many functions to
 *         loading the same field we're almost certainly
 *     </li>
 * </ul>
 */
public abstract class EsqlScalarFunction extends ScalarFunction implements EvaluatorMapper {
    protected EsqlScalarFunction(Source source) {
        super(source);
    }

    protected EsqlScalarFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }
}
