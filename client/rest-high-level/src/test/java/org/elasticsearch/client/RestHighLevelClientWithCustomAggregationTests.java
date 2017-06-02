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

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.singletonList;

/**
 * Test usage of a custom aggregation provided by a plugin with the {@link RestHighLevelClient}.
 */
public class RestHighLevelClientWithCustomAggregationTests extends RestHighLevelClientWithPluginTestCase {

    private static final String CUSTOM = "custom";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return singletonList(CustomPlugin.class);
    }

    public void testCustomAggregation() throws Exception {
        String aggregationName = randomAlphaOfLengthBetween(5, 10);
        CustomAggregationBuilder customAggregationBuilder = new CustomAggregationBuilder(aggregationName);
        final int customNumber = randomIntBetween(1, 1000);
        customAggregationBuilder.setCustomNumber(customNumber);

        Map<String, Object> metaData = null;
        if (randomBoolean()) {
            metaData = new HashMap<>();
            int metaDataCount = between(0, 10);
            while (metaData.size() < metaDataCount) {
                metaData.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
            customAggregationBuilder.setMetaData(metaData);
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("field", "hello"));
        searchSourceBuilder.aggregation(customAggregationBuilder);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = search(searchRequest);

        Map<String, Aggregation> aggregations = searchResponse.getAggregations().getAsMap();
        assertEquals(1, aggregations.size());
        assertTrue(aggregations.containsKey(aggregationName));

        CustomAggregation customAggregation = (CustomAggregation) aggregations.get(aggregationName);
        assertEquals(customNumber, customAggregation.getDocCount());
        assertEquals(metaData, customAggregation.getMetaData());
        assertEquals(customNumber % 2 == 1, customAggregation.isOdd());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Response performRequest(HttpEntity httpEntity) throws IOException {
        try (XContentParser parser = createParser(Request.REQUEST_BODY_CONTENT_TYPE.xContent(), httpEntity.getContent())) {
            Map<String, ?> requestAsMap = parser.map();
            assertEquals(2, requestAsMap.size());
            assertTrue("Search request does not contain the term query", requestAsMap.containsKey("query"));
            assertTrue("Search request does not contain the aggregation", requestAsMap.containsKey("aggregations"));

            Map<String, ?> queryAsMap = (Map<String, ?>) requestAsMap.get("query");
            assertEquals(1, queryAsMap.size());
            assertTrue("Query must be a term query", queryAsMap.containsKey("term"));

            Map<String, ?> aggsAsMap = (Map<String, ?>) requestAsMap.get("aggregations");
            assertEquals("Aggregations must contain the custom aggregation", 1, aggsAsMap.size());
            String name = aggsAsMap.keySet().iterator().next();

            Map<String, ?> customAggregationAsMap = (Map<String, ?>) aggsAsMap.get(name);
            assertTrue("Custom aggregation must have the 'custom' type", customAggregationAsMap.containsKey(CUSTOM));
            Map<String, Object> customMetadata = (Map<String, Object>) customAggregationAsMap.get("meta");

            customAggregationAsMap = (Map<String, ?>) customAggregationAsMap.get(CUSTOM);
            assertTrue("Custom aggregation must contain the random number", customAggregationAsMap.containsKey("number"));

            int customNumber = ((Number) customAggregationAsMap.get("number")).intValue();
            boolean isOdd = (customNumber % 2) != 0;

            InternalCustom internal = new InternalCustom(name, isOdd, customNumber, InternalAggregations.EMPTY, null, customMetadata);
            InternalAggregations internalAggregations = new InternalAggregations(singletonList(internal));
            SearchResponse searchResponse =
                    new SearchResponse(
                            new SearchResponseSections(SearchHits.empty(), internalAggregations, null, false, false, null, 1),
                            randomAlphaOfLengthBetween(5, 10), 5, 5, 100, ShardSearchFailure.EMPTY_ARRAY);
            return createResponse(searchResponse);
        }
    }

    /**
     * A plugin that provides a custom aggregation.
     */
    public static class CustomPlugin extends Plugin implements SearchPlugin {

        public CustomPlugin() {
        }

        @Override
        public List<AggregationSpec> getAggregations() {
            return singletonList(new AggregationSpec(CUSTOM, CustomAggregationBuilder::new, null)
                    .addResultParser((p, c) -> ParsedCustom.fromXContent(p, (String) c)));
        }
    }

    interface CustomAggregation extends SingleBucketAggregation {
        boolean isOdd();
    }

    static class CustomAggregationBuilder extends AbstractAggregationBuilder<CustomAggregationBuilder> {

        private int customNumber;

        CustomAggregationBuilder(String name) {
            super(name);
        }

        CustomAggregationBuilder(StreamInput in) throws IOException {
            super(in);
            this.customNumber = in.readInt();
        }

        @Override
        public String getType() {
            return CUSTOM;
        }

        void setCustomNumber(int value) {
            this.customNumber = value;
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("number", customNumber);
            return builder.endObject();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected AggregatorFactory<?> doBuild(SearchContext context,
                                               AggregatorFactory<?> parent,
                                               AggregatorFactories.Builder subFactories) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(customNumber);
        }

        @Override
        protected boolean doEquals(Object obj) {
            CustomAggregationBuilder other = (CustomAggregationBuilder) obj;
            return customNumber == other.customNumber;
        }
    }

    static class InternalCustom extends InternalSingleBucketAggregation implements CustomAggregation {

        private final boolean odd;

        InternalCustom(String name, boolean odd, long docCount, InternalAggregations subAggregations,
                       List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
            super(name, docCount, subAggregations, pipelineAggregators, metaData);
            this.odd = odd;
        }

        @Override
        public String getWriteableName() {
            return CUSTOM;
        }

        @Override
        public boolean isOdd() {
            return odd;
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            super.doXContentBody(builder, params);
            return builder.field("is_odd", odd);
        }

        @Override
        public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations) {
            throw new UnsupportedOperationException();
        }
    }

    static class ParsedCustom extends ParsedSingleBucketAggregation implements CustomAggregation {

        private boolean odd;

        @Override
        public String getType() {
            return CUSTOM;
        }

        @Override
        public boolean isOdd() {
            return odd;
        }

        void setOdd(boolean odd) {
            this.odd = odd;
        }

        private static final ObjectParser<ParsedCustom, QueryParseContext> PARSER;
        static {
            PARSER = new ObjectParser<>(CUSTOM, ParsedCustom::new);
            PARSER.declareBoolean(ParsedCustom::setOdd, new ParseField("is_odd"));
            PARSER.declareLong(ParsedCustom::setDocCount, CommonFields.DOC_COUNT);
            PARSER.declareObject(ParsedCustom::setMetadata, (p, c) -> p.mapOrdered(), CommonFields.META);
        }

        static ParsedCustom fromXContent(XContentParser parser, final String name) throws IOException {
            ParsedCustom aggregation = PARSER.parse(parser, null);
            aggregation.setName(name);
            return aggregation;
        }
    }
}
