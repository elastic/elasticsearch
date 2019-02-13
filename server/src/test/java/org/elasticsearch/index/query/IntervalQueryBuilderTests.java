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

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.intervals.IntervalQuery;
import org.apache.lucene.search.intervals.Intervals;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IntervalQueryBuilderTests extends AbstractQueryTestCase<IntervalQueryBuilder> {

    @Override
    protected IntervalQueryBuilder doCreateTestQueryBuilder() {
        return new IntervalQueryBuilder(STRING_FIELD_NAME, createRandomSource());
    }

    @Override
    public void testUnknownField() throws IOException {
        super.testUnknownField();
    }

    private static final String[] filters = new String[]{
        "containing", "contained_by", "not_containing", "not_contained_by", "not_overlapping"
    };

    private IntervalsSourceProvider.IntervalFilter createRandomFilter() {
        if (randomInt(20) > 18) {
            return new IntervalsSourceProvider.IntervalFilter(createRandomSource(), randomFrom(filters));
        }
        return null;
    }

    private IntervalsSourceProvider createRandomSource() {
        switch (randomInt(20)) {
            case 0:
            case 1:
                int orCount = randomInt(4) + 1;
                List<IntervalsSourceProvider> orSources = new ArrayList<>();
                for (int i = 0; i < orCount; i++) {
                    orSources.add(createRandomSource());
                }
                return new IntervalsSourceProvider.Disjunction(orSources, createRandomFilter());
            case 2:
            case 3:
                int count = randomInt(5) + 1;
                List<IntervalsSourceProvider> subSources = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    subSources.add(createRandomSource());
                }
                boolean ordered = randomBoolean();
                int maxGaps = randomInt(5) - 1;
                IntervalsSourceProvider.IntervalFilter filter = createRandomFilter();
                return new IntervalsSourceProvider.Combine(subSources, ordered, maxGaps, filter);
            default:
                int wordCount = randomInt(4) + 1;
                List<String> words = new ArrayList<>();
                for (int i = 0; i < wordCount; i++) {
                    words.add(randomRealisticUnicodeOfLengthBetween(4, 20));
                }
                String text = String.join(" ", words);
                boolean mOrdered = randomBoolean();
                int maxMGaps = randomInt(5) - 1;
                String analyzer = randomFrom("simple", "keyword", "whitespace");
                return new IntervalsSourceProvider.Match(text, maxMGaps, mOrdered, analyzer, createRandomFilter());
        }
    }

    @Override
    protected void doAssertLuceneQuery(IntervalQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, instanceOf(IntervalQuery.class));
    }

    public void testMatchInterval() throws IOException {

        String json = "{ \"intervals\" : " +
            "{ \"" + STRING_FIELD_NAME + "\" : { \"match\" : { \"query\" : \"Hello world\" } } } }";

        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.unordered(Intervals.term("hello"), Intervals.term("world")));

        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"" + STRING_FIELD_NAME + "\" : { " +
            "       \"match\" : { " +
            "           \"query\" : \"Hello world\"," +
            "           \"max_gaps\" : 40 } } } }";

        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.maxgaps(40, Intervals.unordered(Intervals.term("hello"), Intervals.term("world"))));
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"" + STRING_FIELD_NAME + "\" : { " +
            "       \"match\" : { " +
            "           \"query\" : \"Hello world\"," +
            "           \"ordered\" : true }," +
            "       \"boost\" : 2 } } }";

        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new BoostQuery(new IntervalQuery(STRING_FIELD_NAME,
            Intervals.ordered(Intervals.term("hello"), Intervals.term("world"))), 2);
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"" + STRING_FIELD_NAME + "\" : { " +
            "       \"match\" : { " +
            "           \"query\" : \"Hello world\"," +
            "           \"max_gaps\" : 10," +
            "           \"analyzer\" : \"whitespace\"," +
            "           \"ordered\" : true } } } }";

        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.maxgaps(10, Intervals.ordered(Intervals.term("Hello"), Intervals.term("world"))));
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : " +
            "{ \"" + STRING_FIELD_NAME + "\" : { " +
            "       \"match\" : { " +
            "           \"query\" : \"Hello world\"," +
            "           \"max_gaps\" : 10," +
            "           \"analyzer\" : \"whitespace\"," +
            "           \"ordered\" : true," +
            "           \"filter\" : {" +
            "               \"containing\" : {" +
            "                   \"match\" : { \"query\" : \"blah\" } } } } } } }";

        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.containing(Intervals.maxgaps(10, Intervals.ordered(Intervals.term("Hello"), Intervals.term("world"))),
                                 Intervals.term("blah")));
        assertEquals(expected, builder.toQuery(createShardContext()));
    }

    public void testOrInterval() throws IOException {

        String json = "{ \"intervals\" : { \"" + STRING_FIELD_NAME + "\": {" +
            "       \"any_of\" : { " +
            "           \"intervals\" : [" +
            "               { \"match\" : { \"query\" : \"one\" } }," +
            "               { \"match\" : { \"query\" : \"two\" } } ] } } } }";
        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.or(Intervals.term("one"), Intervals.term("two")));
        assertEquals(expected, builder.toQuery(createShardContext()));

        json = "{ \"intervals\" : { \"" + STRING_FIELD_NAME + "\": {" +
            "       \"any_of\" : { " +
            "           \"intervals\" : [" +
            "               { \"match\" : { \"query\" : \"one\" } }," +
            "               { \"match\" : { \"query\" : \"two\" } } ]," +
            "           \"filter\" : {" +
            "               \"not_containing\" : { \"match\" : { \"query\" : \"three\" } } } } } } }";
        builder = (IntervalQueryBuilder) parseQuery(json);
        expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.notContaining(
                Intervals.or(Intervals.term("one"), Intervals.term("two")),
                Intervals.term("three")));
        assertEquals(expected, builder.toQuery(createShardContext()));
    }

    public void testCombineInterval() throws IOException {

        String json = "{ \"intervals\" : { \"" + STRING_FIELD_NAME + "\": {" +
            "       \"all_of\" : {" +
            "           \"ordered\" : true," +
            "           \"intervals\" : [" +
            "               { \"match\" : { \"query\" : \"one\" } }," +
            "               { \"all_of\" : { " +
            "                   \"ordered\" : false," +
            "                   \"intervals\" : [" +
            "                       { \"match\" : { \"query\" : \"two\" } }," +
            "                       { \"match\" : { \"query\" : \"three\" } } ] } } ]," +
            "           \"max_gaps\" : 30," +
            "           \"filter\" : { " +
            "               \"contained_by\" : { " +
            "                   \"match\" : { " +
            "                       \"query\" : \"SENTENCE\"," +
            "                       \"analyzer\" : \"keyword\" } } } }," +
            "       \"boost\" : 1.5 } } }";
        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query expected = new BoostQuery(new IntervalQuery(STRING_FIELD_NAME,
            Intervals.containedBy(
                    Intervals.maxgaps(30, Intervals.ordered(
                        Intervals.term("one"),
                        Intervals.unordered(Intervals.term("two"), Intervals.term("three")))),
                    Intervals.term("SENTENCE"))), 1.5f);
        assertEquals(expected, builder.toQuery(createShardContext()));

    }

    public void testCombineDisjunctionInterval() throws IOException {
        String json = "{ \"intervals\" : " +
            "{ \"" + STRING_FIELD_NAME + "\": { " +
            "       \"all_of\" : {" +
            "           \"ordered\" : true," +
            "           \"intervals\" : [" +
            "               { \"match\" : { \"query\" : \"atmosphere\" } }," +
            "               { \"any_of\" : {" +
            "                   \"intervals\" : [" +
            "                       { \"match\" : { \"query\" : \"cold\" } }," +
            "                       { \"match\" : { \"query\" : \"outside\" } } ] } } ]," +
            "           \"max_gaps\" : 30," +
            "           \"filter\" : { " +
            "               \"not_contained_by\" : { " +
            "                   \"match\" : { \"query\" : \"freeze\" } } } } } } }";

        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query expected = new IntervalQuery(STRING_FIELD_NAME,
            Intervals.notContainedBy(
                Intervals.maxgaps(30, Intervals.ordered(
                    Intervals.term("atmosphere"),
                    Intervals.or(Intervals.term("cold"), Intervals.term("outside"))
                )),
                Intervals.term("freeze")));
        assertEquals(expected, builder.toQuery(createShardContext()));
    }

    public void testNonIndexedFields() throws IOException {
        IntervalsSourceProvider provider = createRandomSource();
        IntervalQueryBuilder b = new IntervalQueryBuilder("no_such_field", provider);
        assertThat(b.toQuery(createShardContext()), equalTo(new MatchNoDocsQuery()));

        Exception e = expectThrows(IllegalArgumentException.class, () -> {
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

    public void testMultipleProviders() {
        String json = "{ \"intervals\" : { \"" + STRING_FIELD_NAME + "\": { " +
            "\"boost\" : 1," +
            "\"match\" : { \"query\" : \"term1\" }," +
            "\"all_of\" : { \"intervals\" : [ { \"query\" : \"term2\" } ] } }";

        ParsingException e = expectThrows(ParsingException.class, () -> {
            parseQuery(json);
        });
        assertThat(e.getMessage(), equalTo("Only one interval rule can be specified, found [match] and [all_of]"));
    }

    public void testScriptFilter() throws IOException {

        IntervalFilterScript.Factory factory = () -> new IntervalFilterScript() {
            @Override
            public boolean execute(Interval interval) {
                return interval.getStart() > 3;
            }
        };

        ScriptService scriptService = new ScriptService(Settings.EMPTY, Collections.emptyMap(), Collections.emptyMap()){
            @Override
            @SuppressWarnings("unchecked")
            public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                assertEquals(IntervalFilterScript.CONTEXT, context);
                assertEquals(new Script("interval.start > 3"), script);
                return (FactoryType) factory;
            }
        };

        QueryShardContext baseContext = createShardContext();
        QueryShardContext context = new QueryShardContext(baseContext.getShardId(), baseContext.getIndexSettings(),
            null, null, baseContext.getMapperService(), null,
            scriptService,
            null, null, null, null, null, null);

        String json = "{ \"intervals\" : { \"" + STRING_FIELD_NAME + "\": { " +
            "\"match\" : { " +
            "   \"query\" : \"term1\"," +
            "   \"filter\" : { " +
            "       \"script\" : { " +
            "            \"source\" : \"interval.start > 3\" } } } } } }";

        IntervalQueryBuilder builder = (IntervalQueryBuilder) parseQuery(json);
        Query q = builder.toQuery(context);


        IntervalQuery expected = new IntervalQuery(STRING_FIELD_NAME,
            new IntervalsSourceProvider.ScriptFilterSource(Intervals.term("term1"), "interval.start > 3", null));
        assertEquals(expected, q);

    }


}
