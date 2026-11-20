/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class RandomSamplingQueryBuilderTests extends AbstractQueryTestCase<RandomSamplingQueryBuilder> {

    @Override
    protected RandomSamplingQueryBuilder doCreateTestQueryBuilder() {
        double probability = randomDoubleBetween(0.0, 1.0, false);
        var builder = new RandomSamplingQueryBuilder(probability);
        if (randomBoolean()) {
            builder.seed(randomInt());
        }
        if (randomBoolean()) {
            builder.hash(randomInt());
        }
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(RandomSamplingQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        var rsQuery = asInstanceOf(RandomSamplingQuery.class, query);
        assertThat(rsQuery.probability(), equalTo(queryBuilder.probability()));
        assertThat(rsQuery.seed(), equalTo(queryBuilder.seed()));
        int shardSeed = BitMixer.mix(new ShardId(context.index(), context.getShardId()).hashCode());
        assertThat(rsQuery.hash(), equalTo(queryBuilder.hash() ^ shardSeed));
    }

    /**
     * Shards that share a random stream keep the same doc ids: with one document each, all of them or none.
     */
    public void testShardsSampleDifferentDocs() throws IOException {
        int shards = 64;
        RandomSamplingQueryBuilder builder = new RandomSamplingQueryBuilder(0.5);
        IndexSettings indexSettings = createSearchExecutionContext().getIndexSettings();
        int sampled = 0;
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(new Document());
            try (IndexReader reader = writer.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                for (int shard = 0; shard < shards; shard++) {
                    sampled += searcher.count(builder.toQuery(searchExecutionContext(indexSettings, shard)));
                }
            }
        }
        // Independent shards keep all or none of the documents with a probability of 2^-63.
        assertThat(sampled, allOf(greaterThan(0), lessThan(shards)));
    }

    /**
     * Seeds that differ in a low bit must not get each other's streams on neighbouring shards.
     */
    public void testNearbySeedsDrawDifferentStreams() throws IOException {
        int shards = 8;
        int seed = randomInt();
        IndexSettings indexSettings = createSearchExecutionContext().getIndexSettings();
        Set<Integer> streams = new HashSet<>();
        Set<Integer> nearbyStreams = new HashSet<>();
        for (int shard = 0; shard < shards; shard++) {
            SearchExecutionContext context = searchExecutionContext(indexSettings, shard);
            streams.add(stream(new RandomSamplingQueryBuilder(0.5).seed(seed), context));
            nearbyStreams.add(stream(new RandomSamplingQueryBuilder(0.5).seed(seed ^ 1), context));
        }
        assertThat(Sets.intersection(streams, nearbyStreams), empty());
    }

    /**
     * The value whose mix seeds the stream of {@link RandomSamplingQuery}: equal values, equal streams.
     */
    private static int stream(RandomSamplingQueryBuilder builder, SearchExecutionContext context) throws IOException {
        RandomSamplingQuery query = asInstanceOf(RandomSamplingQuery.class, builder.toQuery(context));
        return query.hash() ^ query.seed();
    }

    /**
     * The contexts of {@link AbstractQueryTestCase} are all for shard 0.
     */
    private SearchExecutionContext searchExecutionContext(IndexSettings indexSettings, int shardId) {
        return new SearchExecutionContext(
            shardId,
            0,
            indexSettings,
            null,
            null,
            null,
            MappingLookup.EMPTY,
            null,
            null,
            parserConfig(),
            namedWriteableRegistry(),
            null,
            null,
            () -> 0L,
            null,
            null,
            () -> true,
            null,
            Map.of(),
            null,
            MapperMetrics.NOOP,
            SearchExecutionContextHelper.SHARD_SEARCH_STATS
        );
    }

    @Override
    protected boolean supportsBoost() {
        return false;
    }

    @Override
    protected boolean supportsQueryName() {
        return false;
    }

    @Override
    public void testUnknownField() {
        var json = "{ \""
            + RandomSamplingQueryBuilder.NAME
            + "\" : {\"bogusField\" : \"someValue\", \""
            + RandomSamplingQueryBuilder.PROBABILITY.getPreferredName()
            + "\" : \""
            + randomBoolean()
            + "\", \""
            + RandomSamplingQueryBuilder.SEED.getPreferredName()
            + "\" : \""
            + randomInt()
            + "\", \""
            + RandomSamplingQueryBuilder.HASH.getPreferredName()
            + "\" : \""
            + randomInt()
            + "\" } }";
        var e = expectThrows(XContentParseException.class, () -> parseQuery(json));
        assertTrue(e.getMessage().contains("bogusField"));
    }
}
