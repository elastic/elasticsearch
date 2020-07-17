/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatorBaseTests extends ESSingleNodeTestCase {

    class BogusAggregator extends AggregatorBase {
        BogusAggregator(SearchContext searchContext, Aggregator parent) throws IOException {
            super("bogus", AggregatorFactories.EMPTY, searchContext, parent, CardinalityUpperBound.NONE, Map.of());
        }

        @Override
        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            throw new UnsupportedOperationException();
        }
    }

    private SearchContext mockSearchContext(Query query) {
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.query()).thenReturn(query);
        BigArrays mockBigArrays = mock(BigArrays.class);
        CircuitBreakerService mockCBS = mock(CircuitBreakerService.class);
        when(mockCBS.getBreaker(CircuitBreaker.REQUEST)).thenReturn(mock(CircuitBreaker.class));
        when(mockBigArrays.breakerService()).thenReturn(mockCBS);
        when(searchContext.bigArrays()).thenReturn(mockBigArrays);
        return searchContext;
    }

    private Function<byte[], Number> pointReaderShim(SearchContext context, Aggregator parent, ValuesSourceConfig config)
        throws IOException {
        BogusAggregator aggregator = new BogusAggregator(context, parent);
        return aggregator.pointReaderIfAvailable(config);
    }

    private Aggregator mockAggregator() {
        return mock(Aggregator.class);
    }

    private ValuesSourceConfig getVSConfig(
        String fieldName,
        NumberFieldMapper.NumberType numType,
        boolean indexed,
        QueryShardContext context
    ) {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(fieldName, numType, indexed, true, Collections.emptyMap());
        return ValuesSourceConfig.resolveFieldOnly(ft, context);
    }

    private ValuesSourceConfig getVSConfig(
        String fieldName,
        DateFieldMapper.Resolution resolution,
        boolean indexed,
        QueryShardContext context
    ) {
        MappedFieldType ft = new DateFieldMapper.DateFieldType(fieldName, indexed, true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER, resolution, Collections.emptyMap());
        return ValuesSourceConfig.resolveFieldOnly(ft, context);
    }

    public void testShortcutIsApplicable() throws IOException {
        IndexService indexService = createIndex("index", Settings.EMPTY, "type", "bytes", "type=keyword");
        client().prepareIndex("index").setId("1").setSource("bytes", "abc").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            QueryShardContext context = indexService.newQueryShardContext(0, searcher, () -> 42L, null);

            for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
                assertNotNull(
                    pointReaderShim(mockSearchContext(new MatchAllDocsQuery()), null, getVSConfig("number", type, true, context))
                );
                assertNotNull(pointReaderShim(mockSearchContext(null), null, getVSConfig("number", type, true, context)));
                assertNull(pointReaderShim(mockSearchContext(null), mockAggregator(), getVSConfig("number", type, true, context)));
                assertNull(
                    pointReaderShim(
                        mockSearchContext(new TermQuery(new Term("foo", "bar"))),
                        null,
                        getVSConfig("number", type, true, context)
                    )
                );
                assertNull(pointReaderShim(mockSearchContext(null), mockAggregator(), getVSConfig("number", type, true, context)));
                assertNull(pointReaderShim(mockSearchContext(null), null, getVSConfig("number", type, false, context)));
            }
            for (DateFieldMapper.Resolution resolution : DateFieldMapper.Resolution.values()) {
                assertNull(
                    pointReaderShim(
                        mockSearchContext(new MatchAllDocsQuery()),
                        mockAggregator(),
                        getVSConfig("number", resolution, true, context)
                    )
                );
                assertNull(
                    pointReaderShim(
                        mockSearchContext(new TermQuery(new Term("foo", "bar"))),
                        null,
                        getVSConfig("number", resolution, true, context)
                    )
                );
                assertNull(pointReaderShim(mockSearchContext(null), mockAggregator(), getVSConfig("number", resolution, true, context)));
                assertNull(pointReaderShim(mockSearchContext(null), null, getVSConfig("number", resolution, false, context)));
            }
            // Check that we decode a dates "just like" the doc values instance.
            Instant expected = Instant.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse("2020-01-01T00:00:00Z"));
            byte[] scratch = new byte[8];
            LongPoint.encodeDimension(DateFieldMapper.Resolution.MILLISECONDS.convert(expected), scratch, 0);
            assertThat(
                pointReaderShim(
                    mockSearchContext(new MatchAllDocsQuery()),
                    null,
                    getVSConfig("number", DateFieldMapper.Resolution.MILLISECONDS, true, context)
                ).apply(scratch),
                equalTo(expected.toEpochMilli())
            );
            LongPoint.encodeDimension(DateFieldMapper.Resolution.NANOSECONDS.convert(expected), scratch, 0);
            assertThat(
                pointReaderShim(
                    mockSearchContext(new MatchAllDocsQuery()),
                    null,
                    getVSConfig("number", DateFieldMapper.Resolution.NANOSECONDS, true, context)
                ).apply(scratch),
                equalTo(expected.toEpochMilli())
            );
        }
    }
}
