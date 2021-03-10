/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class RangeQueryBuilderTests extends AbstractQueryTestCase<RangeQueryBuilder> {
    @Override
    protected RangeQueryBuilder doCreateTestQueryBuilder() {
        RangeQueryBuilder query;
        // switch between numeric and date ranges
        switch (randomIntBetween(0, 2)) {
            case 0:
                // use mapped integer field for numeric range queries
                query = new RangeQueryBuilder(randomFrom(
                    INT_FIELD_NAME, INT_RANGE_FIELD_NAME, INT_ALIAS_FIELD_NAME));
                query.from(randomIntBetween(1, 100));
                query.to(randomIntBetween(101, 200));
                break;
            case 1:
                // use mapped date field, using date string representation
                Instant now = Instant.now();
                ZonedDateTime start = now.minusMillis(randomIntBetween(0, 1000000)).atZone(ZoneOffset.UTC);
                ZonedDateTime end = now.plusMillis(randomIntBetween(0, 1000000)).atZone(ZoneOffset.UTC);
                query = new RangeQueryBuilder(randomFrom(
                    DATE_FIELD_NAME, DATE_RANGE_FIELD_NAME, DATE_ALIAS_FIELD_NAME));
                query.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(start));
                query.to(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(end));
                // Create timestamp option only then we have a date mapper,
                // otherwise we could trigger exception.
                if (createSearchExecutionContext().getFieldType(DATE_FIELD_NAME) != null) {
                    if (randomBoolean()) {
                        query.timeZone(randomZone().getId());
                    }
                    if (randomBoolean()) {
                        String format = "strict_date_optional_time";
                        query.format(format);
                    }
                }
                break;
            case 2:
            default:
                query = new RangeQueryBuilder(randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME));
                query.from("a" + randomAlphaOfLengthBetween(1, 10));
                query.to("z" + randomAlphaOfLengthBetween(1, 10));
                break;
        }
        query.includeLower(randomBoolean()).includeUpper(randomBoolean());
        if (randomBoolean()) {
            query.from(null);
        }
        if (randomBoolean()) {
            query.to(null);
        }
        if (query.fieldName().equals(INT_RANGE_FIELD_NAME) || query.fieldName().equals(DATE_RANGE_FIELD_NAME)) {
            query.relation(
                    randomFrom(ShapeRelation.CONTAINS.toString(), ShapeRelation.INTERSECTS.toString(), ShapeRelation.WITHIN.toString()));
        }
        return query;
    }

    @Override
    protected Map<String, RangeQueryBuilder> getAlternateVersions() {
        Map<String, RangeQueryBuilder> alternateVersions = new HashMap<>();
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(INT_FIELD_NAME);
        rangeQueryBuilder.from(randomIntBetween(1, 100)).to(randomIntBetween(101, 200));
        rangeQueryBuilder.includeLower(randomBoolean());
        rangeQueryBuilder.includeUpper(randomBoolean());
        String query =
                "{\n" +
                "    \"range\":{\n" +
                "        \"" + INT_FIELD_NAME + "\": {\n" +
                "            \"" + (rangeQueryBuilder.includeLower() ? "gte" : "gt") + "\": " + rangeQueryBuilder.from() +  ",\n" +
                "            \"" + (rangeQueryBuilder.includeUpper() ? "lte" : "lt") + "\": " + rangeQueryBuilder.to() +  "\n" +
                "        }\n" +
                "    }\n" +
                "}";
        alternateVersions.put(query, rangeQueryBuilder);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(RangeQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
        if (queryBuilder.from() == null && queryBuilder.to() == null) {
            final Query expectedQuery;
            final MappedFieldType resolvedFieldType = context.getFieldType(queryBuilder.fieldName());
            if (resolvedFieldType.hasDocValues()) {
                expectedQuery = new ConstantScoreQuery(new DocValuesFieldExistsQuery(expectedFieldName));
            } else if (context.getFieldType(resolvedFieldType.name())
                .getTextSearchInfo().hasNorms()) {
                expectedQuery = new ConstantScoreQuery(new NormsFieldExistsQuery(expectedFieldName));
            } else {
                expectedQuery = new ConstantScoreQuery(new TermQuery(new Term(FieldNamesFieldMapper.NAME, expectedFieldName)));
            }
            assertThat(query, equalTo(expectedQuery));
        } else if (expectedFieldName.equals(DATE_FIELD_NAME) == false &&
                        expectedFieldName.equals(INT_FIELD_NAME) == false &&
                        expectedFieldName.equals(DATE_RANGE_FIELD_NAME) == false &&
                        expectedFieldName.equals(INT_RANGE_FIELD_NAME) == false) {
            assertThat(query, instanceOf(TermRangeQuery.class));
            TermRangeQuery termRangeQuery = (TermRangeQuery) query;
            assertThat(termRangeQuery.getField(), equalTo(expectedFieldName));
            assertThat(termRangeQuery.getLowerTerm(), equalTo(BytesRefs.toBytesRef(queryBuilder.from())));
            assertThat(termRangeQuery.getUpperTerm(), equalTo(BytesRefs.toBytesRef(queryBuilder.to())));
            assertThat(termRangeQuery.includesLower(), equalTo(queryBuilder.includeLower()));
            assertThat(termRangeQuery.includesUpper(), equalTo(queryBuilder.includeUpper()));
        } else if (expectedFieldName.equals(DATE_FIELD_NAME)) {
            assertThat(query, instanceOf(IndexOrDocValuesQuery.class));
            query = ((IndexOrDocValuesQuery) query).getIndexQuery();
            assertThat(query, instanceOf(PointRangeQuery.class));
            MappedFieldType mappedFieldType = context.getFieldType(expectedFieldName);
            final Long fromInMillis;
            final Long toInMillis;
            // we have to normalize the incoming value into milliseconds since it could be literally anything
            if (mappedFieldType instanceof DateFieldMapper.DateFieldType) {
                fromInMillis = queryBuilder.from() == null ? null :
                    ((DateFieldMapper.DateFieldType) mappedFieldType).parseToLong(queryBuilder.from(),
                        queryBuilder.includeLower(),
                        queryBuilder.getDateTimeZone(),
                        queryBuilder.getForceDateParser(), context::nowInMillis);
                toInMillis = queryBuilder.to() == null ? null :
                    ((DateFieldMapper.DateFieldType) mappedFieldType).parseToLong(queryBuilder.to(),
                        queryBuilder.includeUpper(),
                        queryBuilder.getDateTimeZone(),
                        queryBuilder.getForceDateParser(), context::nowInMillis);
            } else {
                fromInMillis = toInMillis = null;
                fail("unexpected mapped field type: [" + mappedFieldType.getClass() + "] " + mappedFieldType.toString());
            }

            Long min = fromInMillis;
            Long max = toInMillis;
            long minLong, maxLong;
            if (min == null) {
                minLong = Long.MIN_VALUE;
            } else {
                minLong = min.longValue();
                if (queryBuilder.includeLower() == false && minLong != Long.MAX_VALUE) {
                    minLong++;
                }
            }
            if (max == null) {
                maxLong = Long.MAX_VALUE;
            } else {
                maxLong = max.longValue();
                if (queryBuilder.includeUpper() == false && maxLong != Long.MIN_VALUE) {
                    maxLong--;
                }
            }
            assertEquals(LongPoint.newRangeQuery(DATE_FIELD_NAME, minLong, maxLong), query);
        } else if (expectedFieldName.equals(INT_FIELD_NAME)) {
            assertThat(query, instanceOf(IndexOrDocValuesQuery.class));
            query = ((IndexOrDocValuesQuery) query).getIndexQuery();
            assertThat(query, instanceOf(PointRangeQuery.class));
            Integer min = (Integer) queryBuilder.from();
            Integer max = (Integer) queryBuilder.to();
            int minInt, maxInt;
            if (min == null) {
                minInt = Integer.MIN_VALUE;
            } else {
                minInt = min.intValue();
                if (queryBuilder.includeLower() == false && minInt != Integer.MAX_VALUE) {
                    minInt++;
                }
            }
            if (max == null) {
                maxInt = Integer.MAX_VALUE;
            } else {
                maxInt = max.intValue();
                if (queryBuilder.includeUpper() == false && maxInt != Integer.MIN_VALUE) {
                    maxInt--;
                }
            }
        } else if (expectedFieldName.equals(DATE_RANGE_FIELD_NAME) || expectedFieldName.equals(INT_RANGE_FIELD_NAME)) {
            // todo can't check RangeFieldQuery because its currently package private (this will change)
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new RangeQueryBuilder((String) null));
        expectThrows(IllegalArgumentException.class, () -> new RangeQueryBuilder(""));

        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("test");
        expectThrows(IllegalArgumentException.class, () -> rangeQueryBuilder.timeZone(null));
        expectThrows(IllegalArgumentException.class, () -> rangeQueryBuilder.timeZone("badID"));
        expectThrows(IllegalArgumentException.class, () -> rangeQueryBuilder.format(null));
        expectThrows(IllegalArgumentException.class, () -> rangeQueryBuilder.format("badFormat"));
    }

    public void testToQueryNumericField() throws IOException {
        Query parsedQuery = rangeQuery(INT_FIELD_NAME).from(23).to(54).includeLower(true).includeUpper(false)
            .toQuery(createSearchExecutionContext());
        // since age is automatically registered in data, we encode it as numeric
        assertThat(parsedQuery, instanceOf(IndexOrDocValuesQuery.class));
        parsedQuery = ((IndexOrDocValuesQuery) parsedQuery).getIndexQuery();
        assertThat(parsedQuery, instanceOf(PointRangeQuery.class));
        assertEquals(IntPoint.newRangeQuery(INT_FIELD_NAME, 23, 53), parsedQuery);
    }

    public void testDateRangeQueryFormat() throws IOException {
        // We test 01/01/2012 from gte and 2030 for lt
        String query = "{\n" +
                "    \"range\" : {\n" +
                "        \"" + DATE_FIELD_NAME + "\" : {\n" +
                "            \"gte\": \"01/01/2012\",\n" +
                "            \"lt\": \"2030\",\n" +
                "            \"format\": \"dd/MM/yyyy||yyyy\"\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Query parsedQuery = parseQuery(query).toQuery(createSearchExecutionContext());
        assertThat(parsedQuery, instanceOf(IndexOrDocValuesQuery.class));
        parsedQuery = ((IndexOrDocValuesQuery) parsedQuery).getIndexQuery();
        assertThat(parsedQuery, instanceOf(PointRangeQuery.class));

        assertEquals(LongPoint.newRangeQuery(DATE_FIELD_NAME,
                DateTime.parse("2012-01-01T00:00:00.000+00").getMillis(),
                DateTime.parse("2030-01-01T00:00:00.000+00").getMillis() - 1),
                parsedQuery);

        // Test Invalid format
        final String invalidQuery = "{\n" +
                "    \"range\" : {\n" +
                "        \"" + DATE_FIELD_NAME + "\" : {\n" +
                "            \"gte\": \"01/01/2012\",\n" +
                "            \"lt\": \"2030\",\n" +
                "            \"format\": \"yyyy\"\n" +
                "        }\n" +
                "    }\n" +
                "}";
        expectThrows(ElasticsearchParseException.class, () -> parseQuery(invalidQuery).toQuery(createSearchExecutionContext()));
    }

    public void testDateRangeBoundaries() throws IOException {
        String query = "{\n" +
                "    \"range\" : {\n" +
                "        \"" + DATE_FIELD_NAME + "\" : {\n" +
                "            \"gte\": \"2014-11-05||/M\",\n" +
                "            \"lte\": \"2014-12-08||/d\"\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        Query parsedQuery = parseQuery(query).toQuery(createSearchExecutionContext());
        assertThat(parsedQuery, instanceOf(IndexOrDocValuesQuery.class));
        parsedQuery = ((IndexOrDocValuesQuery) parsedQuery).getIndexQuery();
        assertThat(parsedQuery, instanceOf(PointRangeQuery.class));
        assertEquals(LongPoint.newRangeQuery(DATE_FIELD_NAME,
                DateTime.parse("2014-11-01T00:00:00.000+00").getMillis(),
                DateTime.parse("2014-12-08T23:59:59.999+00").getMillis()),
                parsedQuery);

        query = "{\n" +
                "    \"range\" : {\n" +
                "        \"" + DATE_FIELD_NAME + "\" : {\n" +
                "            \"gt\": \"2014-11-05||/M\",\n" +
                "            \"lt\": \"2014-12-08||/d\"\n" +
                "        }\n" +
                "    }\n" +
                "}";
        parsedQuery = parseQuery(query).toQuery(createSearchExecutionContext());
        assertThat(parsedQuery, instanceOf(IndexOrDocValuesQuery.class));
        parsedQuery = ((IndexOrDocValuesQuery) parsedQuery).getIndexQuery();
        assertThat(parsedQuery, instanceOf(PointRangeQuery.class));
        assertEquals(LongPoint.newRangeQuery(DATE_FIELD_NAME,
                DateTime.parse("2014-11-30T23:59:59.999+00").getMillis() + 1,
                DateTime.parse("2014-12-08T00:00:00.000+00").getMillis() - 1),
                parsedQuery);
    }

    public void testDateRangeQueryTimezone() throws IOException {
        String query = "{\n" +
                "    \"range\" : {\n" +
                "        \"" + DATE_FIELD_NAME + "\" : {\n" +
                "            \"gte\": \"2012-01-01\",\n" +
                "            \"lte\": \"now\",\n" +
                "            \"time_zone\": \"+01:00\"\n" +
                "        }\n" +
                "    }\n" +
                "}";
        SearchExecutionContext context = createSearchExecutionContext();
        Query parsedQuery = parseQuery(query).toQuery(context);
        assertThat(parsedQuery, instanceOf(DateRangeIncludingNowQuery.class));
        parsedQuery = ((DateRangeIncludingNowQuery)parsedQuery).getQuery();
        assertThat(parsedQuery, instanceOf(IndexOrDocValuesQuery.class));
        parsedQuery = ((IndexOrDocValuesQuery) parsedQuery).getIndexQuery();
        assertThat(parsedQuery, instanceOf(PointRangeQuery.class));
        // TODO what else can we assert

        query = "{\n" +
                "    \"range\" : {\n" +
                "        \"" + INT_FIELD_NAME + "\" : {\n" +
                "            \"gte\": \"0\",\n" +
                "            \"lte\": \"100\",\n" +
                "            \"time_zone\": \"-01:00\"\n" +
                "        }\n" +
                "    }\n" +
                "}";
        QueryBuilder queryBuilder = parseQuery(query);
        queryBuilder.toQuery(createSearchExecutionContext()); // no exception
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"range\" : {\n" +
                "    \"timestamp\" : {\n" +
                "      \"from\" : \"2015-01-01 00:00:00\",\n" +
                "      \"to\" : \"now\",\n" +
                "      \"include_lower\" : true,\n" +
                "      \"include_upper\" : true,\n" +
                "      \"time_zone\" : \"+01:00\",\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";

        RangeQueryBuilder parsed = (RangeQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "2015-01-01 00:00:00", parsed.from());
        assertEquals(json, "now", parsed.to());
    }

    public void testNamedQueryParsing() throws IOException {
        String json =
                "{\n" +
                "  \"range\" : {\n" +
                "    \"timestamp\" : {\n" +
                "      \"from\" : \"2015-01-01 00:00:00\",\n" +
                "      \"to\" : \"now\",\n" +
                "      \"boost\" : 1.0,\n" +
                "      \"_name\" : \"my_range\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        assertNotNull(parseQuery(json));
    }

    public void testRewriteDateToMatchAll() throws IOException {
        String fieldName = DATE_FIELD_NAME;
        RangeQueryBuilder query = new RangeQueryBuilder(fieldName) {
            @Override
            protected MappedFieldType.Relation getRelation(QueryRewriteContext queryRewriteContext) {
                return Relation.WITHIN;
            }
        };
        DateTime queryFromValue = new DateTime(2015, 1, 1, 0, 0, 0, ISOChronology.getInstanceUTC());
        DateTime queryToValue = new DateTime(2016, 1, 1, 0, 0, 0, ISOChronology.getInstanceUTC());
        query.from(queryFromValue);
        query.to(queryToValue);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder rewritten = query.rewrite(searchExecutionContext);
        assertThat(rewritten, instanceOf(RangeQueryBuilder.class));
        RangeQueryBuilder rewrittenRange = (RangeQueryBuilder) rewritten;
        assertThat(rewrittenRange.fieldName(), equalTo(fieldName));
        assertThat(rewrittenRange.from(), equalTo(null));
        assertThat(rewrittenRange.to(), equalTo(null));

        // Range query with open bounds rewrite to an exists query
        Query luceneQuery = rewrittenRange.toQuery(searchExecutionContext);
        final Query expectedQuery;
        if (searchExecutionContext.getFieldType(query.fieldName()).hasDocValues()) {
            expectedQuery = new ConstantScoreQuery(new DocValuesFieldExistsQuery(query.fieldName()));
        } else {
            expectedQuery = new ConstantScoreQuery(new TermQuery(new Term(FieldNamesFieldMapper.NAME, query.fieldName())));
        }
        assertThat(luceneQuery, equalTo(expectedQuery));

        SearchExecutionContext searchExecutionContextWithUnkType = createShardContextWithNoType();
        luceneQuery  = rewrittenRange.toQuery(searchExecutionContextWithUnkType);
        assertThat(luceneQuery, equalTo(new MatchNoDocsQuery("no mappings yet")));
    }

    public void testRewriteDateToMatchAllWithTimezoneAndFormat() throws IOException {
        String fieldName = DATE_FIELD_NAME;
        RangeQueryBuilder query = new RangeQueryBuilder(fieldName) {
            @Override
            protected MappedFieldType.Relation getRelation(QueryRewriteContext queryRewriteContext) {
                return Relation.WITHIN;
            }
        };
        DateTime queryFromValue = new DateTime(2015, 1, 1, 0, 0, 0, ISOChronology.getInstanceUTC());
        DateTime queryToValue = new DateTime(2016, 1, 1, 0, 0, 0, ISOChronology.getInstanceUTC());
        query.from(queryFromValue);
        query.to(queryToValue);
        query.timeZone(randomZone().getId());
        query.format("yyyy-MM-dd");
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder rewritten = query.rewrite(searchExecutionContext);
        assertThat(rewritten, instanceOf(RangeQueryBuilder.class));
        RangeQueryBuilder rewrittenRange = (RangeQueryBuilder) rewritten;
        assertThat(rewrittenRange.fieldName(), equalTo(fieldName));
        assertThat(rewrittenRange.from(), equalTo(null));
        assertThat(rewrittenRange.to(), equalTo(null));
        assertThat(rewrittenRange.timeZone(), equalTo(null));
        assertThat(rewrittenRange.format(), equalTo(null));
    }

    public void testRewriteDateToMatchNone() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(1, 20);
        RangeQueryBuilder query = new RangeQueryBuilder(fieldName) {
            @Override
            protected MappedFieldType.Relation getRelation(QueryRewriteContext queryRewriteContext) {
                return Relation.DISJOINT;
            }
        };
        DateTime queryFromValue = new DateTime(2015, 1, 1, 0, 0, 0, ISOChronology.getInstanceUTC());
        DateTime queryToValue = new DateTime(2016, 1, 1, 0, 0, 0, ISOChronology.getInstanceUTC());
        query.from(queryFromValue);
        query.to(queryToValue);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder rewritten = query.rewrite(searchExecutionContext);
        assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testRewriteDateToSame() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(1, 20);
        RangeQueryBuilder query = new RangeQueryBuilder(fieldName) {
            @Override
            protected MappedFieldType.Relation getRelation(QueryRewriteContext queryRewriteContext) {
                return Relation.INTERSECTS;
            }
        };
        DateTime queryFromValue = new DateTime(2015, 1, 1, 0, 0, 0, ISOChronology.getInstanceUTC());
        DateTime queryToValue = new DateTime(2016, 1, 1, 0, 0, 0, ISOChronology.getInstanceUTC());
        query.from(queryFromValue);
        query.to(queryToValue);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder rewritten = query.rewrite(searchExecutionContext);
        assertThat(rewritten, sameInstance(query));
    }

    public void testRewriteOpenBoundsToSame() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(1, 20);
        RangeQueryBuilder query = new RangeQueryBuilder(fieldName) {
            @Override
            protected MappedFieldType.Relation getRelation(QueryRewriteContext queryRewriteContext) {
                return Relation.INTERSECTS;
            }
        };
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder rewritten = query.rewrite(searchExecutionContext);
        assertThat(rewritten, sameInstance(query));
    }

    public void testParseFailsWithMultipleFields() {
        String json =
                "{\n" +
                "    \"range\": {\n" +
                "      \"age\": {\n" +
                "        \"gte\": 30,\n" +
                "        \"lte\": 40\n" +
                "      },\n" +
                "      \"price\": {\n" +
                "        \"gte\": 10,\n" +
                "        \"lte\": 30\n" +
                "      }\n" +
                "    }\n" +
                "  }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[range] query doesn't support multiple fields, found [age] and [price]", e.getMessage());
    }

    public void testParseFailsWithMultipleFieldsWhenOneIsDate() {
        String json =
                "{\n" +
                "    \"range\": {\n" +
                "      \"age\": {\n" +
                "        \"gte\": 30,\n" +
                "        \"lte\": 40\n" +
                "      },\n" +
                "      \"" + DATE_FIELD_NAME + "\": {\n" +
                "        \"gte\": \"2016-09-13 05:01:14\"\n" +
                "      }\n" +
                "    }\n" +
                "  }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[range] query doesn't support multiple fields, found [age] and [" + DATE_FIELD_NAME + "]", e.getMessage());
    }

    public void testParseRelation() {
        String json =
            "{\n" +
                "    \"range\": {\n" +
                "      \"age\": {\n" +
                "        \"gte\": 30,\n" +
                "        \"lte\": 40,\n" +
                "        \"relation\": \"disjoint\"\n" +
                "      }" +
                "    }\n" +
                "  }";
        String fieldName = randomAlphaOfLengthBetween(1, 20);
        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> parseQuery(json));
        assertEquals("[range] query does not support relation [disjoint]", e1.getMessage());
        RangeQueryBuilder builder = new RangeQueryBuilder(fieldName);
        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, ()->builder.relation("disjoint"));
        assertEquals("[range] query does not support relation [disjoint]", e2.getMessage());
        builder.relation("contains");
        assertEquals(ShapeRelation.CONTAINS, builder.relation());
        builder.relation("within");
        assertEquals(ShapeRelation.WITHIN, builder.relation());
        builder.relation("intersects");
        assertEquals(ShapeRelation.INTERSECTS, builder.relation());
    }

    /**
     * Range queries should generally be cacheable, at least the ones we create randomly.
     * This test makes sure we also test the non-cacheable cases regularly.
     */
    @Override
    public void testCacheability() throws IOException {
        RangeQueryBuilder queryBuilder = createTestQueryBuilder();
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());

        // queries on date fields using "now" should not be cached
        queryBuilder = new RangeQueryBuilder(randomFrom(DATE_FIELD_NAME, DATE_RANGE_FIELD_NAME, DATE_ALIAS_FIELD_NAME));
        queryBuilder.to("now");
        context = createSearchExecutionContext();
        rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }
}
