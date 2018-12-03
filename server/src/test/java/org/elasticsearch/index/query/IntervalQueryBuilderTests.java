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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.intervals.IntervalQuery;
import org.apache.lucene.search.intervals.Intervals;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IntervalQueryBuilderTests extends AbstractQueryTestCase<IntervalQueryBuilder> {

    @Override
    protected IntervalQueryBuilder doCreateTestQueryBuilder() {
        return new IntervalQueryBuilder(STRING_FIELD_NAME, createRandomSource());
    }

    private IntervalsSourceProvider createRandomSource() {
        switch (randomInt(20)) {
            case 0:
                IntervalsSourceProvider source1 = createRandomSource();
                IntervalsSourceProvider source2 = createRandomSource();
                int relOrd = randomInt(IntervalsSourceProvider.Relate.Relation.values().length - 1);
                return new IntervalsSourceProvider.Relate(source1, source2, IntervalsSourceProvider.Relate.Relation.values()[relOrd]);
            case 1:
                int orCount = randomInt(4) + 1;
                List<IntervalsSourceProvider> orSources = new ArrayList<>();
                for (int i = 0; i < orCount; i++) {
                    orSources.add(createRandomSource());
                }
                return new IntervalsSourceProvider.Disjunction(orSources);
            case 2:
            case 3:
                int count = randomInt(5) + 1;
                List<IntervalsSourceProvider> subSources = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    subSources.add(createRandomSource());
                }
                int typeOrd = randomInt(IntervalsSourceProvider.CombineType.values().length - 1);
                int width = randomBoolean() ? Integer.MAX_VALUE : randomIntBetween(count, 100);
                return new IntervalsSourceProvider.Combine(subSources, IntervalsSourceProvider.CombineType.values()[typeOrd], width);
            default:
                int wordCount = randomInt(4) + 1;
                List<String> words = new ArrayList<>();
                for (int i = 0; i < wordCount; i++) {
                    words.add(randomRealisticUnicodeOfLengthBetween(4, 20));
                }
                String text = String.join(" ", words);
                int mtypeOrd = randomInt(MappedFieldType.IntervalType.values().length - 1);
                MappedFieldType.IntervalType type = MappedFieldType.IntervalType.values()[mtypeOrd];
                return new IntervalsSourceProvider.Match(text, randomBoolean() ? Integer.MAX_VALUE : randomIntBetween(1, 20), type);
        }
    }

    @Override
    protected void doAssertLuceneQuery(IntervalQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, instanceOf(IntervalQuery.class));
    }

    public void testMatchInterval() throws IOException {

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
            "                       \"type\" : \"ordered\" } } } }";
        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.ordered(Intervals.term("hello"), Intervals.term("world")));
        assertEquals(expected, builder.toQuery(createShardContext()));

    }

    public void testOrInterval() throws IOException {
        String json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\", " +
            "  \"source\" : { " +
            "       \"or\" : {" +
            "           \"sources\": [" +
            "               { \"match\" : { \"text\" : \"one\" } }," +
            "               { \"match\" : { \"text\" : \"two\" } } ] } } } }";
        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.or(Intervals.term("one"), Intervals.term("two")));
        assertEquals(expected, builder.toQuery(createShardContext()));
    }

    public void testCombineInterval() throws IOException {

        String json = "{ \"intervals\" : " +
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
        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.maxwidth(30, Intervals.ordered(
                Intervals.term("one"),
                Intervals.unordered(Intervals.term("two"), Intervals.term("three")))));
        assertEquals(expected, builder.toQuery(createShardContext()));

    }

    public void testCombineDisjunctionInterval() throws IOException {
        String json = "{ \"intervals\" : " +
            "{ \"field\" : \"" + STRING_FIELD_NAME + "\", " +
            "  \"source\" : { " +
            "       \"combine\" : {" +
            "           \"type\" : \"ordered\"," +
            "           \"sources\" : [" +
            "               { \"match\" : { \"text\" : \"atmosphere\" } }," +
            "               { \"or\" : {" +
            "                   \"sources\" : [" +
            "                       { \"match\" : { \"text\" : \"cold\" } }," +
            "                       { \"match\" : { \"text\" : \"outside\" } } ] } } ]," +
            "           \"max_width\" : 30 } } } }";

        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.maxwidth(30, Intervals.ordered(
                Intervals.term("atmosphere"),
                Intervals.or(Intervals.term("cold"), Intervals.term("outside"))
            )));
        assertEquals(expected, builder.toQuery(createShardContext()));
    }

    public void testRelateIntervals() throws IOException {

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

    public void testNonIndexedFields() {
        IntervalsSourceProvider provider = createRandomSource();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            IntervalQueryBuilder builder = new IntervalQueryBuilder("no_such_field", provider);
            builder.doToQuery(createShardContext());
        });
        assertThat(e.getMessage(), equalTo("Cannot create IntervalQuery over non-existent field [no_such_field]"));

        e = expectThrows(IllegalArgumentException.class, () -> {
            IntervalQueryBuilder builder = new IntervalQueryBuilder(INT_FIELD_NAME, provider);
            builder.doToQuery(createShardContext());
        });
        assertThat(e.getMessage(), equalTo("Cannot create IntervalQuery over field [" + INT_FIELD_NAME + "] with no indexed positions"));

        e = expectThrows(IllegalArgumentException.class, () -> {
            IntervalQueryBuilder builder = new IntervalQueryBuilder(STRING_FIELD_NAME_2, provider);
            builder.doToQuery(createShardContext());
        });
        assertThat(e.getMessage(), equalTo("Cannot create IntervalQuery over field ["
            + STRING_FIELD_NAME_2 + "] with no indexed positions"));
    }
}
