/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.List;
import org.elasticsearch.core.Map;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatorBaseTests extends MapperServiceTestCase {

    class BogusAggregator extends AggregatorBase {
        BogusAggregator(AggregationContext context, Aggregator parent) throws IOException {
            super("bogus", AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, Map.of());
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

    private AggregationContext context(Query query) {
        AggregationContext context = mock(AggregationContext.class);
        when(context.query()).thenReturn(query);
        when(context.breaker()).thenReturn(mock(CircuitBreaker.class));
        return context;
    }

    private Function<byte[], Number> pointReaderShim(AggregationContext context, Aggregator parent, ValuesSourceConfig config)
        throws IOException {
        return new BogusAggregator(context, parent).pointReaderIfAvailable(config);
    }

    private Aggregator mockAggregator() {
        return mock(Aggregator.class);
    }

    private ValuesSourceConfig getVSConfig(
        String fieldName,
        NumberFieldMapper.NumberType numType,
        boolean indexed,
        AggregationContext context
    ) {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(
            fieldName,
            numType,
            indexed,
            false,
            true,
            false,
            null,
            Collections.emptyMap(),
            null
        );
        return ValuesSourceConfig.resolveFieldOnly(ft, context);
    }

    private ValuesSourceConfig getVSConfig(
        String fieldName,
        DateFieldMapper.Resolution resolution,
        boolean indexed,
        AggregationContext context
    ) {
        MappedFieldType ft = new DateFieldMapper.DateFieldType(
            fieldName,
            indexed,
            false,
            true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            resolution,
            null,
            null,
            Collections.emptyMap()
        );
        return ValuesSourceConfig.resolveFieldOnly(ft, context);
    }

    public void testShortcutIsApplicable() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", "abc"))), context -> {
            for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
                assertNotNull(pointReaderShim(context(new MatchAllDocsQuery()), null, getVSConfig("number", type, true, context)));
                assertNotNull(pointReaderShim(context(null), null, getVSConfig("number", type, true, context)));
                assertNull(pointReaderShim(context(null), mockAggregator(), getVSConfig("number", type, true, context)));
                assertNull(
                    pointReaderShim(context(new TermQuery(new Term("foo", "bar"))), null, getVSConfig("number", type, true, context))
                );
                assertNull(pointReaderShim(context(null), mockAggregator(), getVSConfig("number", type, true, context)));
                assertNull(pointReaderShim(context(null), null, getVSConfig("number", type, false, context)));
            }
            for (DateFieldMapper.Resolution resolution : DateFieldMapper.Resolution.values()) {
                assertNull(
                    pointReaderShim(context(new MatchAllDocsQuery()), mockAggregator(), getVSConfig("number", resolution, true, context))
                );
                assertNull(
                    pointReaderShim(context(new TermQuery(new Term("foo", "bar"))), null, getVSConfig("number", resolution, true, context))
                );
                assertNull(pointReaderShim(context(null), mockAggregator(), getVSConfig("number", resolution, true, context)));
                assertNull(pointReaderShim(context(null), null, getVSConfig("number", resolution, false, context)));
            }
            // Check that we decode a dates "just like" the doc values instance.
            Instant expected = Instant.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse("2020-01-01T00:00:00Z"));
            byte[] scratch = new byte[8];
            LongPoint.encodeDimension(DateFieldMapper.Resolution.MILLISECONDS.convert(expected), scratch, 0);
            assertThat(
                pointReaderShim(
                    context(new MatchAllDocsQuery()),
                    null,
                    getVSConfig("number", DateFieldMapper.Resolution.MILLISECONDS, true, context)
                ).apply(scratch),
                equalTo(expected.toEpochMilli())
            );
            LongPoint.encodeDimension(DateFieldMapper.Resolution.NANOSECONDS.convert(expected), scratch, 0);
            assertThat(
                pointReaderShim(
                    context(new MatchAllDocsQuery()),
                    null,
                    getVSConfig("number", DateFieldMapper.Resolution.NANOSECONDS, true, context)
                ).apply(scratch),
                equalTo(expected.toEpochMilli())
            );
        });
    }
}
