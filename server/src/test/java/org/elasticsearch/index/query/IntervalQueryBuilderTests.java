package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.intervals.IntervalQuery;
import org.apache.lucene.search.intervals.Intervals;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class IntervalQueryBuilderTests extends AbstractQueryTestCase<IntervalQueryBuilder> {

    @Override
    protected IntervalQueryBuilder doCreateTestQueryBuilder() {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        IntervalsSourceProvider match1 = new IntervalsSourceProvider.Match("jabber crackpot henceforth", Integer.MAX_VALUE, true);
        IntervalsSourceProvider match2 = new IntervalsSourceProvider.Match("floo", Integer.MAX_VALUE, true);
        IntervalsSourceProvider combi
            = new IntervalsSourceProvider.Combine(Arrays.asList(match1, match2), IntervalsSourceProvider.Combine.Type.ORDERED, 30);
        return new IntervalQueryBuilder(STRING_FIELD_NAME, combi);
    }

    @Override
    protected void doAssertLuceneQuery(IntervalQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, instanceOf(IntervalQuery.class));
    }

    public void testMatchInterval() throws IOException {

        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);

        String json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\"," +
            "  \"source\" : { \"match\" : { " +
            "                       \"text\" : \"Hello world\" } } } }";

        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        IntervalQuery expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.unordered(Intervals.term("hello"), Intervals.term("world")));

        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\"," +
            "  \"source\" : { \"match\" : { " +
            "                       \"text\" : \"Hello world\"," +
            "                       \"max_width\" : 40 } } } }";
        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.maxwidth(40, Intervals.unordered(Intervals.term("hello"), Intervals.term("world"))));
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\"," +
            "  \"source\" : { \"match\" : { " +
            "                       \"text\" : \"Hello world\"," +
            "                       \"ordered\" : \"true\" } } } }";
        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.ordered(Intervals.term("hello"), Intervals.term("world")));
        assertEquals(expected, builder.toQuery(createShardContext()));

    }

    public void testCombineInterval() throws IOException {

        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);

        String json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\", " +
            "  \"source\" : { " +
            "       \"combine\" : {" +
            "           \"type\" : \"or\"," +
            "           \"sources\" : [" +
            "               { \"match\" : { \"text\" : \"one\" } }," +
            "               { \"match\" : { \"text\" : \"two\" } } ]," +
            "           \"max_width\" : 30 } } } }";
        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.maxwidth(30, Intervals.or(Intervals.term("one"), Intervals.term("two"))));
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\", " +
            "  \"source\" : { " +
            "       \"combine\" : {" +
            "           \"type\" : \"ordered\"," +
            "           \"sources\" : [" +
            "               { \"match\" : { \"text\" : \"one\" } }," +
            "               { \"combine\" : { " +
            "                   \"type\" : \"unordered\"," +
            "                   \"sources\" : [" +
            "                       { \"match\" : { \"text\" : \"two\" } }," +
            "                       { \"match\" : { \"text\" : \"three\" } } ] } } ]," +
            "           \"max_width\" : 30 } } } }";
        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.maxwidth(30, Intervals.ordered(
                Intervals.term("one"),
                Intervals.unordered(Intervals.term("two"), Intervals.term("three")))));
        assertEquals(expected, builder.toQuery(createShardContext()));

    }

    public void testRelateIntervals() throws IOException {

        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);

        String json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\", " +
            "  \"source\" : { " +
            "       \"relate\" : {" +
            "           \"relation\" : \"containing\"," +
            "           \"source\" : { \"match\" : { \"text\" : \"one\" } }," +
            "           \"filter\" : { \"match\" : { \"text\" : \"two\" } } } } } }";
        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.containing(Intervals.term("one"), Intervals.term("two")));
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\", " +
            "  \"source\" : { " +
            "       \"relate\" : {" +
            "           \"relation\" : \"contained_by\"," +
            "           \"source\" : { \"match\" : { \"text\" : \"one\" } }," +
            "           \"filter\" : { \"match\" : { \"text\" : \"two\" } } } } } }";
        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.containedBy(Intervals.term("one"), Intervals.term("two")));
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\", " +
            "  \"source\" : { " +
            "       \"relate\" : {" +
            "           \"relation\" : \"not_containing\"," +
            "           \"source\" : { \"match\" : { \"text\" : \"one\" } }," +
            "           \"filter\" : { \"match\" : { \"text\" : \"two\" } } } } } }";
        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.notContaining(Intervals.term("one"), Intervals.term("two")));
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\", " +
            "  \"source\" : { " +
            "       \"relate\" : {" +
            "           \"relation\" : \"not_contained_by\"," +
            "           \"source\" : { \"match\" : { \"text\" : \"one\" } }," +
            "           \"filter\" : { \"match\" : { \"text\" : \"two\" } } } } } }";
        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.notContainedBy(Intervals.term("one"), Intervals.term("two")));
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\", " +
            "  \"source\" : { " +
            "       \"relate\" : {" +
            "           \"relation\" : \"not_overlapping\"," +
            "           \"source\" : { \"match\" : { \"text\" : \"one\" } }," +
            "           \"filter\" : { \"match\" : { \"text\" : \"two\" } } } } } }";
        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.nonOverlapping(Intervals.term("one"), Intervals.term("two")));
        assertEquals(expected, builder.toQuery(createShardContext()));
    }
}
