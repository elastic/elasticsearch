/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.terms;

import com.carrotsearch.hppc.DoubleOpenHashSet;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.ObjectOpenHashSet;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.matchAllFilter;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SimpleTermsByQueryActionTests extends ElasticsearchIntegrationTest {

    /**
     * Tests that the terms by query action returns the correct terms against string fields
     */
    @Test
    public void testTermsByQuery() throws Exception {
        int numShards = randomIntBetween(1, 6);
        int numDocs = randomIntBetween(100, 2000);
        long maxTermsPerShard = randomIntBetween(1, numDocs / numShards - 1);

        logger.info("--> creating index [idx] shards [" + numShards + "]");
        client().admin().indices().prepareCreate("idx").setSettings("index.number_of_shards", numShards, "index.number_of_replicas", 0).execute().actionGet();
        ensureGreen();

        logger.info("--> indexing [" + numDocs + "] docs");
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("idx", "type", "" + i)
                    .setSource(jsonBuilder().startObject()
                            .field("str", Integer.toString(i)).field("dbl", Double.valueOf(i)).field("int", i)
                            .endObject()).execute().actionGet();
        }

        client().admin().indices().prepareRefresh("idx").execute().actionGet();

        logger.info("--> lookup terms in field [str]");
        TermsByQueryResponse resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("str")
                .setFilter(matchAllFilter()).execute().actionGet();

        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is((long) numDocs));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.BytesResponseTerms, is(true));
        assertThat(resp.getResponseTerms().getTerms() instanceof BytesRefHash, is(true));
        BytesRefHash bTerms = (BytesRefHash) resp.getResponseTerms().getTerms();
        assertThat(bTerms.size(), is((long) numDocs));
        for (int i = 0; i < numDocs; i++) {
            BytesRef spare = new BytesRef(Integer.toString(i));
            assertThat(bTerms.find(spare, spare.hashCode()) >= 0, is(true));
        }

        logger.info("--> lookup terms in field [str] with max terms per shard");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("str")
                .setFilter(matchAllFilter()).setMaxTermsPerShard(maxTermsPerShard).execute().actionGet();

        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is(numShards * maxTermsPerShard));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.BytesResponseTerms, is(true));
        assertThat(resp.getResponseTerms().getTerms() instanceof BytesRefHash, is(true));
        bTerms = (BytesRefHash) resp.getResponseTerms().getTerms();
        assertThat(bTerms.size(), is(numShards * maxTermsPerShard));

        logger.info("--> lookup terms in field [str] with BloomFilter");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("str").setUseBloomFilter(true)
                .setExpectedInsertions(numDocs).setFilter(matchAllFilter()).execute().actionGet();

        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is((long) numDocs));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.BloomResponseTerms, is(true));
        assertThat(resp.getResponseTerms().getTerms() instanceof BloomFilter, is(true));
        BloomFilter bloomFilter = (BloomFilter) resp.getResponseTerms().getTerms();
        for (int i = 0; i < numDocs; i++) {
            assertThat(bloomFilter.mightContain(new BytesRef(Integer.toString(i))), is(true));
        }

        logger.info("--> lookup terms in field [str] with BloomFilter and max terms per shard");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("str").setUseBloomFilter(true)
                .setExpectedInsertions(numDocs).setFilter(matchAllFilter()).setMaxTermsPerShard(maxTermsPerShard)
                .execute().actionGet();

        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is(numShards * maxTermsPerShard));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.BloomResponseTerms, is(true));
        assertThat(resp.getResponseTerms().getTerms() instanceof BloomFilter, is(true));

        logger.info("--> lookup terms in field [int]");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("int")
                .setFilter(matchAllFilter()).execute().actionGet();

        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is((long) numDocs));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.LongsResponseTerms, is(true));
        assertThat(resp.getResponseTerms().getTerms() instanceof LongHash, is(true)); // bloom doesn't store terms
        LongHash lTerms = (LongHash) resp.getResponseTerms().getTerms();
        assertThat(lTerms.size(), is((long) numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(lTerms.find(Long.valueOf(i)) >= 0, is(true));
        }

        logger.info("--> lookup terms in field [int] with max terms per shard");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("int")
                .setFilter(matchAllFilter()).setMaxTermsPerShard(maxTermsPerShard).execute().actionGet();

        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is(numShards * maxTermsPerShard));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.LongsResponseTerms, is(true));
        assertThat(resp.getResponseTerms().getTerms() instanceof LongHash, is(true)); // bloom doesn't store terms
        lTerms = (LongHash) resp.getResponseTerms().getTerms();
        assertThat((long) lTerms.size(), is(numShards * maxTermsPerShard));

        logger.info("--> lookup terms in field [dbl]");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("dbl")
                .setFilter(matchAllFilter()).execute().actionGet();

        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is((long) numDocs));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.DoublesResponseTerms, is(true));
        assertThat(resp.getResponseTerms().getTerms() instanceof LongHash, is(true));
        LongHash dTerms = (LongHash) resp.getResponseTerms().getTerms();
        assertThat(dTerms.size(), is((long) numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(dTerms.find(Double.doubleToLongBits(Double.valueOf(i))) >= 0, is(true));
        }
        int found = 0;
        for (long i = 0; i < dTerms.capacity(); i++) {
            final long id = dTerms.id(i);
            if (id >= 0) {
                long lval = dTerms.get(id);
                double dval = Double.longBitsToDouble(lval);
                assertThat(dval >= 0 && dval < numDocs, is(true));
                found++;
            }
        }
        assertThat(found, equalTo(numDocs));

        logger.info("--> lookup terms in field [dbl] with max terms per shard");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("dbl")
                .setFilter(matchAllFilter()).setMaxTermsPerShard(maxTermsPerShard).execute().actionGet();

        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is(numShards * maxTermsPerShard));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.DoublesResponseTerms, is(true));
        assertThat(resp.getResponseTerms().getTerms() instanceof LongHash, is(true));
        dTerms = (LongHash) resp.getResponseTerms().getTerms();
        assertThat((long) dTerms.size(), is(numShards * maxTermsPerShard));

        logger.info("--> lookup in field [str] with no docs");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("str")
                .setFilter(rangeFilter("int").gt(numDocs)).execute().actionGet();
        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is(0L));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.BytesResponseTerms, is(true));
        bTerms = (BytesRefHash) resp.getResponseTerms().getTerms();
        assertThat(bTerms.size(), is(0L));

        logger.info("--> lookup in field [int] with no docs");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("int")
                .setFilter(rangeFilter("int").gt(numDocs)).execute().actionGet();
        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is(0L));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.LongsResponseTerms, is(true));
        lTerms = (LongHash) resp.getResponseTerms().getTerms();
        assertThat(lTerms.size(), is(0L));

        logger.info("--> lookup in field [dbl] with no docs");
        resp = new TermsByQueryRequestBuilder(client()).setIndices("idx").setField("dbl")
                .setFilter(rangeFilter("int").gt(numDocs)).execute().actionGet();
        ElasticsearchAssertions.assertNoFailures(resp);
        assertThat(resp.getResponseTerms(), notNullValue());
        assertThat(resp.getResponseTerms().size(), is(0L));
        assertThat(resp.getResponseTerms() instanceof ResponseTerms.DoublesResponseTerms, is(true));
        dTerms = (LongHash) resp.getResponseTerms().getTerms();
        assertThat(dTerms.size(), is(0L));
    }
}
