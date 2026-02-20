/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.blockloader;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxBytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxLongsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.Utf8CodePointsFromOrdsBlockLoader;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.stats.SearchStats;

/**
 * {@link Expression} that can be "pushed" into value loading. Most of the time
 * we load values into {@link Block}s and then run the expressions on them, but
 * sometimes it's worth short-circuiting this process and running the expression
 * in the tight loop we use for loading values.
 * <ul>
 *     <li>
 *         {@code V_COSINE(vector, [constant_vector])} - a vector is ~512 floats
 *         and V_COSINE is one double. Better yet, we can use the search index
 *         to find the distance without even looking at all the values.
 *     </li>
 *     <li>
 *         {@code ST_CENTROID(shape)} - shapes can be quite large. Centroids
 *         are just one point.
 *     </li>
 *     <li>
 *         {@code LENGTH(string)} - strings can be quite long, but string length
 *         is always an int. For more fun, {@code keyword}s are usually stored
 *         using a dictionary, and it's <strong>fairly</strong> easy to optimize
 *         running {@code LENGTH} once per dictionary entry.
 *     </li>
 *     <li>
 *         {@code MV_COUNT(anything)} - counts are always integers.
 *     </li>
 *     <li>
 *         {@code MV_MIN} and {@code MV_MAX} - loads a single value instead of
 *         multivalued fields.
 *     </li>
 * </ul>
 * <p>
 *     See the docs for {@link EsqlScalarFunction} for how this optimization fits in with
 *     all the other optimizations we've implemented.
 * </p>
 * <h2>How to implement</h2>
 * <ol>
 *     <li>Implement some block loaders</li>
 *     <li>Unit test the block loaders</li>
 *     <li>Plug the {@link BlockLoader} into the {@link MappedFieldType#blockLoader field mapper}</li>
 *     <li>Implement this interface</li>
 *     <li>Add to {@code PushExpressionToLoadIT}</li>
 *     <li>Maybe add to {@code csv-spec} tests</li>
 *     <li>Get some performance numbers and open a PR</li>
 * </ol>
 * <h3>Implement some block loaders</h3>
 * <p>
 *     Implement a {@link BlockLoader} for each fused code path. There's
 *     going to be a {@linkplain BlockLoader} per
 *     {@code <FUNCTION> x <type> x <storage mechanism>}. Examples:
 * </p>
 * <ol>
 *     <li>
 *         {@link Utf8CodePointsFromOrdsBlockLoader} is for {@code LENGTH x keyword x docValues}.
 *     </li>
 *     <li>
 *         {@link MvMaxLongsFromDocValuesBlockLoader} is for {@code MV_MAX x long x docValues}.
 *     </li>
 *     <li>
 *         {@link MvMaxBytesRefsFromOrdsBlockLoader} is for {@code MV_MAX x (keyword|ip) x doc_values}.
 *     </li>
 * </ol>
 * <p>
 *     If you wanted to push <strong>all</strong> loads for a function applied
 *     to a field type you'd need to optimize all paths which could include:
 * </p>
 * <ol>
 *     <li>{@code doc_values}</li>
 *     <li>{@code stored}</li>
 *     <li>{@code _source}</li>
 *     <li>Funky synthetic {@code _source} cases</li>
 *     <li>Using the search index</li>
 * </ol>
 * <p>
 *     Unless you have a good reason to do otherwise, it's generally fine to start with
 *     {@code doc_values}. And it might be fine to only implement this fusion
 *     for {@code doc_values}. <strong>Usually</strong>, loading {@code stored} fields
 *     and loading from {@code _source} is so slow that this optimization won't buy you
 *     much speed <strong>proportionally</strong>. But this is only a rule of thumb.
 *     The first extraction push down we implemented violates the rule! It was directly
 *     to the search index for vector fields.
 * </p>
 * <p>
 *     Note: The {@link Object#toString}s are important in these classes. We expose them
 *     over the {@code profile} API and use them for tests later on.
 * </p>
 * <h2>Unit test the block loaders</h2>
 * <p>
 *     Build a randomized unit test that
 * </p>
 * <ol>
 *     <li>loads random data</li>
 *     <li>loads using both your new {@link BlockLoader} and the non-fused loader</li>
 *     <li>compares the results</li>
 * </ol>
 * <p>
 *     See the test for {@link Utf8CodePointsFromOrdsBlockLoader} for an example. These tests
 *     are usually quite parameterized to make sure we cover things like:
 * </p>
 * <ul>
 *     <li>
 *         loading column-at-a-time ({@link BlockLoader.ColumnAtATimeReader#read}) vs
 *         loading row-by-row ({@link BlockLoader.RowStrideReader#read})
 *     </li>
 *     <li>high vs load cardinality values</li>
 *     <li>all documents have only single-valued fields vs some have multivalued fields</li>
 *     <li>some documents are missing values</li>
 * </ul>
 * <p>
 *     These unit tests cover a ton of different configurations quickly, and we
 *     <strong>know</strong> that we're using the loader.
 * </p>
 * <h2>Plug the {@link BlockLoader} into the {@link MappedFieldType#blockLoader field mapper}</h2>
 * <p>
 *     You must implement:
 * </p>
 * <ul>
 *     <li>
 *         {@link MappedFieldType#supportsBlockLoaderConfig} to control <strong>if</strong>
 *         the field is pushed down. Return {@code true} for the configurations that match
 *         your {@link BlockLoader}s.
 *     </li>
 *     <li>
 *         {@link MappedFieldType#blockLoader} to control <strong>how</strong> the field
 *         is pushed down. Return your {@link BlockLoader}s here.
 *     </li>
 * </ul>
 * <h2>Implement this interface</h2>
 * <p>
 *     Implement {@link BlockLoaderExpression}. Generally it's enough to check that
 *     check if the function is being applied to a {@link FieldAttribute} and do something
 *     like:
 * </p>
 * <pre>{@code
 *         if (field instanceof FieldAttribute f && f.dataType() == DataType.KEYWORD) {
 *             return new PushedBlockLoaderExpression(f, BlockLoaderFunctionConfig.Function.WHATEVER);
 *         }
 *         return null;
 * }</pre>
 * <p>
 *     The rules system will check {@link MappedFieldType#supportsBlockLoaderConfig} for you.
 *     See the docs for {@link #tryPushToFieldLoading} for more on how to implement it.
 * </p>
 * <h2>Add to {@code PushExpressionToLoadIT}</h2>
 * <p>
 *     Add a case or two to {@code PushExpressionToLoadIT} to prove that we've plugged
 *     everything in properly. These tests make sure that we're really loading the data
 *     really using your new {@linkplain BlockLoader}. This is where your nice
 *     {@link Object#toString}s come into play. That's the key into the profile map that
 *     shows that your new {@linkplain BlockLoader} is plugged in.
 * </p>
 * <h2>Maybe add to {@code csv-spec} tests</h2>
 * <p>
 *     Look for your function in the csv-spec tests and make sure there are cases that
 *     contain your function processing each data type you are pushing. For each type,
 *     make sure the function processes the results of:
 * </p>
 * <ul>
 *     <li>{@code ROW} - these <strong>won't</strong> use your new code</li>
 *     <li>{@code FROM} - these <strong>will</strong> use your new code</li>
 *     <li>{@code STATS} or another function - these <strong>won't</strong> use your new code</li>
 * </ul>
 * <p>
 *     It's <strong>fairly</strong> likely we already have tests for all these cases.
 *     They are part of our standard practice for adding functions, but there are a lot
 *     of them, and we may have forgotten some. And, without the pushdown you are
 *     implementing, they are mostly there for healthy paranoia around rules and
 *     a hedge against mistakes implementing optimizations in the future. Like the
 *     optimization you are implementing now!
 * </p>
 * <p>
 *     Anyway, once there are plenty of these tests you should run them via the ESQL
 *     unit tests and via the single-node integration tests. These tests don't prove
 *     that your new {@linkplain BlockLoader}s are plugged in. You have
 *     {@code PushExpressionToLoadIT} for that. Instead, they prove that, when your
 *     new {@linkplain BlockLoader} <strong>is</strong> plugged in, it produces
 *     correct output. So, just like your unit test, but integrated with the entire
 *     rest of the world.
 * </p>
 * <h2>Get some performance numbers and open a PR</h2>
 * <p>
 *     Now that you can be pretty sure everything is plugged in and working you can
 *     get some performance numbers. It's generally good to start with a quick and
 *     <a href="https://gist.github.com/nik9000/f46b39a6213902c10e92ff51a7924e5c">dirty script</a>.
 *     These should show you a performance improvement, and you can use the
 *     {@code profile} API as a final proof that everything is plugged in. Once that
 *     looks right you should generally be ok to open a PR. Attach the results of
 *     your bash script to prove that it's faster.
 * </p>
 * <p>
 *     Next, look for a <a href="https://github.com/elastic/rally-tracks">rally track</a>
 *     that should improve with your PR. If you find one, <strong>and</strong> it's
 *     in the nightlies already, then you have a choice:
 * </p>
 * <ul>
 *     <li>Run the rally tests right now to get better numbers</li>
 *     <li>Wait for the nightlies to run after merging</li>
 * </ul>
 * <p>
 *     If the quick and dirty perf testing looked good you are probably safe waiting on
 *     the nightlies. You should look for them in
 *     <a href="https://benchmarks.elastic.co/index.html">benchmarks.elastic.co</a>.
 * </p>
 * <p>
 *     If there isn't already a rally operation then you should add one like this
 *     <a href="https://github.com/elastic/rally-tracks/pull/902">PR</a>. How you add
 *     one of these and how you get it into the nightlies and whether it should be in
 *     the nightlies is outside the scope of this document.
 * </p>
 */
public interface BlockLoaderExpression {
    /**
     * The field and loading configuration that replaces this expression, effectively
     * "fusing" the expression into the load. Or null if the fusion isn't possible.
     * <p>
     *     {@link SearchStats#supportsLoaderConfig} checks that the configuration is
     *     supported by all field mappers. Callers to this method <strong>must</strong>
     *     call that to confirm that configurations returned are supported. Implementations
     *     of this method do not need to call it, though they <strong>may</strong> use
     *     methods like {@link SearchStats#hasDocValues} and {@link SearchStats#isIndexed}
     *     as preflight checks. They <strong>should</strong> use those methods if it is
     *     expensive to build the {@link BlockLoaderFunctionConfig}.
     * </p>
     */
    @Nullable
    PushedBlockLoaderExpression tryPushToFieldLoading(SearchStats stats);

    /**
     * Expression "fused" to the block loader.
     * @param field the field whose load we're fusing into
     * @param config the expression's configuration
     */
    record PushedBlockLoaderExpression(FieldAttribute field, BlockLoaderFunctionConfig config) {
        public PushedBlockLoaderExpression(FieldAttribute field, BlockLoaderFunctionConfig.Function function) {
            this(field, new BlockLoaderFunctionConfig.JustFunction(function));
        }
    }
}
