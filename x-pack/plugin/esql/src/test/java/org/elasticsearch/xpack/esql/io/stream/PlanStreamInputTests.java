/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PlanStreamInputTests extends ESTestCase {

    public void testMapperSimple() {
        var mapper = new PlanStreamInput.NameIdMapper();

        NameId first = mapper.apply(1L);
        NameId second = mapper.apply(1L);
        assertThat(second, equalTo(first));

        NameId third = mapper.apply(2L);
        NameId fourth = mapper.apply(2L);
        assertThat(third, not(equalTo(second)));
        assertThat(fourth, equalTo(third));

        assertThat(mapper.seen.size(), is(2));
    }

    public void testMapper() {
        List<Long> longs = randomLongsListOfSize(100);
        List<Long> nameIds = new ArrayList<>();
        for (long l : longs) {
            nameIds.add(l);
            if (randomBoolean()) { // randomly insert additional values from the known list
                int idx = randomIntBetween(0, longs.size() - 1);
                nameIds.add(longs.get(idx));
            }
        }

        var mapper = new PlanStreamInput.NameIdMapper();
        List<NameId> mappedIds = nameIds.stream().map(mapper::apply).toList();
        assertThat(mappedIds.size(), is(nameIds.size()));
        // there must be exactly 100 distinct elements
        assertThat(mapper.seen.size(), is(100));
        assertThat(mappedIds.stream().distinct().count(), is(100L));

        // The pre-mapped name id pattern must match that of the mapped one
        Map<Long, List<Long>> nameIdsSeen = new LinkedHashMap<>(); // insertion order
        for (int i = 0; i < nameIds.size(); i++) {
            long value = nameIds.get(i);
            nameIdsSeen.computeIfAbsent(value, k -> new ArrayList<>());
            nameIdsSeen.get(value).add((long) i);
        }
        assert nameIdsSeen.size() == 100;

        Map<NameId, List<Long>> mappedSeen = new LinkedHashMap<>(); // insertion order
        for (int i = 0; i < mappedIds.size(); i++) {
            NameId nameId = mappedIds.get(i);
            mappedSeen.computeIfAbsent(nameId, k -> new ArrayList<>());
            mappedSeen.get(nameId).add((long) i);
        }
        assert mappedSeen.size() == 100;

        var mappedSeenItr = mappedSeen.values().iterator();
        for (List<Long> indexes : nameIdsSeen.values()) {
            assertThat(indexes, equalTo(mappedSeenItr.next()));
        }
    }

    List<Long> randomLongsListOfSize(int size) {
        Set<Long> longs = new HashSet<>();
        while (longs.size() < size) {
            longs.add(randomLong());
        }
        return longs.stream().toList();
    }

    public void testSourceSerialization() {
        Function<String, String> queryFn = delimiter -> delimiter
            + "FROM "
            + delimiter
            + " test "
            + delimiter
            + "| EVAL "
            + delimiter
            + " x = CONCAT(first_name, \"baz\")"
            + delimiter
            + "| EVAL last_name IN (\"foo\", "
            + delimiter
            + " \"bar\")"
            + delimiter
            + "| "
            + delimiter
            + "WHERE emp_no == abs("
            + delimiter
            + "emp_no)"
            + delimiter;

        Function<LogicalPlan, List<Source>> sources = plan -> {
            List<Expression> exp = new ArrayList<>();
            plan.forEachDown(p -> {
                if (p instanceof Eval e) {
                    e.fields().forEach(a -> exp.add(a.child()));
                } else if (p instanceof Filter f) {
                    exp.add(f.condition());
                }
            });
            return exp.stream().map(Expression::source).toList();
        };

        for (var delim : new String[] { "", "\r", "\n", "\r\n" }) {
            String query = queryFn.apply(delim);
            Configuration config = configuration(query);

            LogicalPlan planIn = analyze(query);
            LogicalPlan planOut = serializeDeserialize(
                planIn,
                PlanStreamOutput::writeNamedWriteable,
                in -> in.readNamedWriteable(LogicalPlan.class),
                config
            );
            assertThat(planIn, equalTo(planOut));
            assertThat(sources.apply(planIn), equalTo(sources.apply(planOut)));
        }
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
