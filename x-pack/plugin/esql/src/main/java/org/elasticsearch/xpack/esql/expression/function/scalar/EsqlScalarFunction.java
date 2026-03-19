/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.MvEvaluator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.lucene.query.LuceneCountOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNToSource;

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
 *     different classes of optimizations for them that exist on a performance spectrum:
 * </p>
 * <pre>{@code
 *  Better         Load Less and
 * than O(rows)     Run Faster               Run Faster                 Page-at-a-time     Tuple-at-a-time
 *     |----------------|-------------------------|------------------------------|-------------------|
 *     ^  ^  ^     ^    ^      ^                  ^           ^    ^   ^     ^   ^      ^            ^
 *    CF LT ET    FP   BL     MBL                SE          NO  SIMD RR    VD EVAL    EVE         CASE
 * }</pre>
 * <h3>{@code CF}: Constant Folding</h3>
 * <pre>{@code
 *   | EVAL a = CONCAT("some ", "words")
 * }</pre>
 * <p>
 *     The fastest way to run a scalar, now and forever, is to run it at compile time. Turn it
 *     into a constant and propagate it throughout the query. This is called "constant folding"
 *     and all scalars, when their arguments are constants, are "folded" to a constant.
 * </p>
 * <h3>{@code LT}: Lucene's TopN</h3>
 * <pre>{@code
 *     FROM index METADATA _score
 *   | WHERE title:"cat"
 *   | SORT _score DESC
 *   | LIMIT 10
 * }</pre>
 * <pre>{@code
 *     FROM index
 *   | EVAL distance = ST_DISTANCE(point, "POINT(12.5683 55.6761)")
 *   | SORT distance ASC
 *   | LIMIT 10
 * }</pre>
 * <p>
 *     Fundamentally, Lucene is a tuple-at-a-time engine that flows the
 *     <a href="https://www.elastic.co/blog/faster-retrieval-of-top-hits-in-elasticsearch-with-block-max-wand">min-competitive</a>
 *     sort key back into the index iteration process, allowing it to skip huge swaths of
 *     documents. It has quite a few optimizations that soften the blow of it being
 *     tuple-at-a-time, so these days "push to a lucene topn" is the fastest way you are going
 *     to run a scalar function. For that to work it has to be a {@code SORT} key and all the
 *     filters have to be pushable to lucene and lucene has to know how to run the function
 *     natively. See {@link PushTopNToSource}.
 * </p>
 * <h3>{@code ET}: Engine TopN (<strong>HYPOTHETICAL</strong>)</h3>
 * <pre>{@code
 *     FROM index METADATA _score
 *   | WHERE title:"cat"
 *   | WHERE a < j + LENGTH(candy) // <--- anything un-pushable
 *   | SORT _score DESC
 *   | LIMIT 10
 * }</pre>
 * <p>
 *     If ESQL's {@link TopNOperator} exposed the min-competitive information (see above), and
 *     we fed it back into the lucene query operators then we too could do better than
 *     {@code O(matching_rows)} for queries sorting on the results of a scalar. This is like
 *     the {@code LT} but without as many limitations. Lucene has a 20-year head start on us
 *     optimizing TopN, so we should continue to use them when
 *     See <a href="https://github.com/elastic/elasticsearch/issues/136267">issue</a>.
 * </p>
 * <h3>{@code BL}: Push to {@link BlockLoader}</h3>
 * <pre>{@code
 *     FROM index
 *   | EVAL s = V_COSINE(dense_vector, [0, 1, 2])
 *   | SORT s desc
 *   | LIMIT 10
 * }</pre>
 * <pre>{@code
 *     FROM index
 *   | STATS SUM(LENGTH(message)) // Length is pushed to the BlockLoader
 * }</pre>
 * <p>
 *     Some functions can take advantage of the on-disk structures to run very fast and should be
 *     "fused" into field loading using {@link BlockLoaderExpression}. Functions like {@code V_COSINE}
 *     can use the vector search index to compute the result. Functions like {@code MV_MIN} can
 *     use the {@code doc_values} encoding mechanism to save a ton of work. Functions like the
 *     upcoming {@code ST_SIMPLIFY} benefit from this by saving huge numbers of allocations even
 *     if they can't link into the {@code doc_values} format. We do this by building a
 *     {@link BlockLoader} for each {@code FUNCTION x FIELD_TYPE x storage mechanism} combination
 *     so we can get as much speed as possible.
 * </p>
 * <h3>{@code MBL}: Push to a "mother ship" {@link BlockLoader} (<strong>HYPOTHETICAL</strong>)</h3>
 * <pre>{@code
 *     FROM index
 *   | STATS SUM(LENGTH(message)), // All of these are pushed to a single BlockLoader
 *           SUM(SUBSTRING(message, 0, 4)),
 *        BY trail = SUBSTRING(message, 10, 3)
 * }</pre>
 * <p>
 *     Pushing functions to a {@link BlockLoader} can involve building a <strong>ton</strong>
 *     of distinct {@link BlockLoader}s. Which involves a ton of code and testing and, well, work.
 *     But it's worth it if you are applying a single function to a field and every single cycle
 *     counts. Both of these cry out for a more OO-style solution where you build a "mother ship"
 *     {@linkplain BlockLoader} that operates on, say {@code FIELD_TYPE x storage mechanism} and
 *     then runs a list of {@code FUNCTION} operations. In some cases this is a bad idea, which
 *     is why we haven't built it yet. But in plenty of cases it's fine. And, sometimes, we should
 *     be fine skipping the special purpose block loader in favor of the mother ship. We'd spent
 *     a few more cycles on each load, but the maintenance advantage is likely worth it for some
 *     functions.
 * </p>
 * <h3>{@code EVAL}: Page-at-a-time evaluation</h3>
 * <p>
 *     ESQL evaluates whole pages at once, generally walking a couple of arrays in parallel building
 *     a result array. This makes which bits are the "hot path" very obvious - they are the loops
 *     that walk these arrays. We put the "slower" stuff outside those loops:
 * </p>
 * <ul>
 *     <li>scratch allocations</li>
 *     <li>profiling</li>
 * </ul>
 * <h3>{@code VD}: Vector Dispatch</h3>
 * <p>
 *     In Elasticsearch it's normal for fields to sometimes be {@code null} or multivalued.
 *     There are no constraints on the schema preventing this and, as a search engine, it's
 *     pretty normal to model things as multivalued fields. We rarely know that a field can
 *     only be single-valued when we're planning a query.
 * </p>
 * <p>
 *     It's <strong>much</strong> faster to run a scalar when we know that all of its inputs
 *     are single valued and non-null. So every scalar function that uses the code generation
 *     keyed by the {@link Evaluator}, {@link ConvertEvaluator}, and {@link MvEvaluator}
 *     annotations builds two paths:
 * </p>
 * <ul>
 *     <li>The slower "{@link Block}" path that supports {@code null}s and multivalued fields</li>
 *     <li>The faster "{@link Vector}" path that supports only single-valued, non-{@code null} fields</li>
 * </ul>
 * <h3>{@code NO}: Native Ordinal Evaluation</h3>
 * <pre>{@code
 *     FROM index
 *   | STATS MAX(foo) BY TO_UPPER(verb)
 * }</pre>
 * <p>
 *     {@code keyword} and {@code ip} fields load their {@code byte[]} shaped values as a
 *     lookup table, called "ordinals" because Lucene uses that word for it. Some of our functions,
 *     like {@code TO_UPPER}, process the lookup table itself instead of processing each position.
 *     This is especially important when grouping on the field because the hashing done by the
 *     aggregation code also operates on the lookup table.
 * </p>
 * <h3>{@code SE}: Sorted Execution</h3>
 * <pre>{@code
 *     FROM index
 *   | STATS SUM(MV_DEDUPE(file_size))
 * }</pre>
 * <p>
 *     Some functions can operate on multivalued fields much faster if their inputs are sorted. And
 *     inputs loaded from {@code doc_values} are sorted by default. Sometimes even sorted AND
 *     deduplicated. We store this information on each block in {@link Block.MvOrdering}.
 * </p>
 * <p>
 *     NOTE: Functions that can take advantage of this sorting also tend to be NOOPs for
 *     single-valued inputs. So they benefit hugely from "Vector Dispatch".
 * </p>
 * <h3>{@code SIMD}: Single Instruction Multiple Data instructions</h3>
 * <pre>{@code
 *     FROM index
 *   | STATS MAX(lhs + rhs)
 * }</pre>
 * <p>
 *     Through a combination of "Page-at-a-time evaluation", and "Vector Dispatch" we often
 *     end up with at least one path that can be turned into a sequence of
 *     <a href="https://en.wikipedia.org/wiki/Single_instruction,_multiple_data">SIMD</a>
 *     instructions. These are about as fast as you can go and still be `O(matching_rows)`.
 *     A lot of scalars don't lend themselves perfectly to SIMD, but we make sure those
 *     that do can take that route.
 * </p>
 * <h3>{@code RR}: Range Rewrite</h3>
 * <pre>{@code
 *     FROM index
 *   | STATS COUNT(*) BY DATE_TRUNC(1 DAY, @timestamp)
 * }</pre>
 * <p>
 *     Functions like {@code DATE_TRUNC} can be quite slow, especially when they are using a
 *     time zone. It can be much faster if it knows the range of dates that it's operating on.
 *     And we do know that on the data node! We use that information to rewrite the possibly-slow
 *     {@code DATE_TRUNC} to the always fast {@code ROUND_TO}, which rounds down to fixed rounding
 *     points.
 * </p>
 * <p>
 *     At the moment this is only done for {@code DATE_TRUNC} which is a very common function,
 *     but is technically possible for anything that could benefit from knowing the range up front.
 * </p>
 * <h3>{@code FP}: Filter Pushdown</h3>
 * <pre>{@code
 *     FROM index
 *   | STATS COUNT(*) BY DATE_TRUNC(1 DAY, @timestamp)
 * }</pre>
 * <p>
 *     If the "Range Rewrite" optimization works, we can sometimes further push the resulting
 *     {@code ROUND_TO} into a sequence of filters. If you are <strong>just</strong> counting
 *     documents then this can use the {@link LuceneCountOperator} which can count the number of
 *     matching documents directly from the cache, <strong>technically</strong> being faster than
 *     {@code O(num_hits)}, but only in ideal circumstances. If we can't push the count then it's
 *     still very very fast. See <a href="https://github.com/elastic/elasticsearch/pull/138023">PR</a>.
 * </p>
 * <h3>{@code EVE}: Expensive Variable Evaluator</h3>
 * <pre>{@code
 *     FROM index
 *   | EVAL ts = DATE_PARSE(SUBSTRING(message, 1, 10), date_format_from_the_index)
 * }</pre>
 * <p>
 *     Functions like {@code DATE_PARSE} need to build something "expensive" per input row, like
 *     a {@link DateFormatter}. But, often, the expensive thing is constant. In the example above
 *     the date format comes from the index, but that's quite contrived. These functions generally
 *     run in the form:
 * </p>
 * <pre>{@code
 *     FROM index
 *   | EVAL ts = DATE_PARSE(SUBSTRING(message, 1, 10), "ISO8601")
 * }</pre>
 * <p>
 *     These generally have special case evaluators that don't construct the format for each row.
 *     The others are "expensive variable evaluators" and we avoid them when we can.
 * </p>
 * <h3>{@code CASE}: {@code CASE} is evaluated row-by-row</h3>
 * <pre>{@code
 *     FROM index
 *   | EVAL f = CASE(d > 0, n / d, 0)
 * }</pre>
 * <pre>{@code
 *     FROM index
 *   | EVAL f = COALESCE(d, 1 / j)
 * }</pre>
 * <p>
 *     {@code CASE} and {@code COALESCE} short circuit. In the top example above, that
 *     means we don't run {@code n / d} unless {@code d > 0}. That prevents us from
 *     emitting warnings for dividing by 0. In the second example, we don't run {@code 1 / j}
 *     unless {@code d} is null. In the worst case, we manage this by running row-by-row
 *     which is <strong>super</strong> slow. Especially because the engine was designed
 *     for page-at-a-time execution.
 * </p>
 * <p>
 *     In the best case {@code COALESCE} can see that an input is either all-null or
 *     all-non-null. Then it never falls back to row-by-row evaluation and is quite fast.
 * </p>
 * <p>
 *     {@code CASE} has a similar optimization: For each incoming {@link Page}, if the
 *     condition evaluates to a constant, then it executes the corresponding "arm"
 *     Page-at-a-time. Also! If the "arms" are "fast" and can't throw warnings, then
 *     {@code CASE} can execute "eagerly" - evaluating all three arguments and just
 *     plucking values back and forth. The "eager" {@code CASE} evaluator is effectively
 *     the same as any other page-at-a-time evaluator.
 * </p>
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
