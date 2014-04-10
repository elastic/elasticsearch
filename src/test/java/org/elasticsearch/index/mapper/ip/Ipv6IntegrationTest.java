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

package org.elasticsearch.index.mapper.ip;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.math.BigInteger;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class Ipv6IntegrationTest extends ElasticsearchIntegrationTest {

    @Test
    public void testIndexIpv6() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("ipv6")
                        .field("type", "ipv6")
                        .field("precision_step", randomIntBetween(4, 8))
                    .endObject()
                .endObject()
                .endObject().endObject().string();

        createIndex("test");
        admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();

        BigInteger ip1 = getRandomIp();

        XContentBuilder doc1 = XContentFactory.jsonBuilder()
                .startObject()
                .field("ipv6", Ipv6FieldMapper.numberToIPv6(ip1))
                .endObject();


        index("test", "type", "1", doc1);

        refresh();
        RangeQueryBuilder rangeQueryBuilder;
        RangeFilterBuilder rangeFilterBuilder;
        SearchResponse searchResponse;

        // test term query
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.termQuery("ipv6", Ipv6FieldMapper.numberToIPv6(ip1))).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));

        // test term query not match
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.termQuery("ipv6", Ipv6FieldMapper.numberToIPv6(ip1.add(BigInteger.ONE)))).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        // test in range
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .from(Ipv6FieldMapper.numberToIPv6(ip1.subtract(BigInteger.ONE)))
                .to(Ipv6FieldMapper.numberToIPv6(ip1.add(BigInteger.ONE)))
                .includeLower(true)
                .includeUpper(true);
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));


        // test not in range
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .from(Ipv6FieldMapper.numberToIPv6(ip1.add(BigInteger.valueOf(10l))))
                .to(Ipv6FieldMapper.numberToIPv6(ip1.add(BigInteger.valueOf(20l))))
                .includeLower(true)
                .includeUpper(true);
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        // test is lower bound
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .from(Ipv6FieldMapper.numberToIPv6(ip1))
                .to(Ipv6FieldMapper.numberToIPv6(ip1.add(BigInteger.ONE)))
                .includeLower(true)
                .includeUpper(true);
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));

        // test is lower bound but exclude lower bound
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .from(Ipv6FieldMapper.numberToIPv6(ip1))
                .to(Ipv6FieldMapper.numberToIPv6(ip1.add(BigInteger.ONE)))
                .includeLower(false)
                .includeUpper(true);
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        // test is upper bound
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .from(Ipv6FieldMapper.numberToIPv6(ip1.subtract(BigInteger.ONE)))
                .to(Ipv6FieldMapper.numberToIPv6(ip1))
                .includeLower(true)
                .includeUpper(true);
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));

        // test is upper bound but exclude upper bound
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .from(Ipv6FieldMapper.numberToIPv6(ip1.subtract(BigInteger.ONE)))
                .to(Ipv6FieldMapper.numberToIPv6(ip1))
                .includeLower(true)
                .includeUpper(false);
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        // test no lower bound and in range
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .to(Ipv6FieldMapper.numberToIPv6(ip1.add(BigInteger.ONE)));
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));

        // test no lower bound and not in range
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .to(Ipv6FieldMapper.numberToIPv6(ip1.subtract(BigInteger.ONE)));
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        // test no upper bound and in range
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .from(Ipv6FieldMapper.numberToIPv6(ip1.subtract(BigInteger.ONE)));
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));

        // test no upper bound and not in range
        rangeQueryBuilder = QueryBuilders.rangeQuery("ipv6")
                .from(Ipv6FieldMapper.numberToIPv6(ip1.add(BigInteger.ONE)));
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(rangeQueryBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        // test filter
        rangeFilterBuilder = FilterBuilders.rangeFilter("ipv6")
                .from(Ipv6FieldMapper.numberToIPv6(ip1.subtract(BigInteger.ONE)))
                .to(Ipv6FieldMapper.numberToIPv6(ip1.add(BigInteger.ONE)))
                .includeLower(true)
                .includeUpper(true);
        searchResponse = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(rangeFilterBuilder).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
    }

    public BigInteger getRandomIp() {
        BigInteger max = BigInteger.ONE.shiftLeft(128).subtract(BigInteger.ONE);
        BigInteger bigInteger;
        do {
            bigInteger = new BigInteger(129, getRandom());
        } while (bigInteger.compareTo(max) >= 0);
        return bigInteger;
    }


}
