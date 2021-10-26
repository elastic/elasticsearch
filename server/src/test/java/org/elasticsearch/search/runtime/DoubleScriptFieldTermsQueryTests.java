/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.elasticsearch.script.Script;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;

public class DoubleScriptFieldTermsQueryTests extends AbstractDoubleScriptFieldQueryTestCase<DoubleScriptFieldTermsQuery> {
    @Override
    protected DoubleScriptFieldTermsQuery createTestInstance() {
        LongSet terms = new LongHashSet();
        int count = between(1, 100);
        while (terms.size() < count) {
            terms.add(Double.doubleToLongBits(randomDouble()));
        }
        return new DoubleScriptFieldTermsQuery(randomScript(), leafFactory, randomAlphaOfLength(5), terms);
    }

    @Override
    protected DoubleScriptFieldTermsQuery copy(DoubleScriptFieldTermsQuery orig) {
        LongSet terms = new LongHashSet();
        for (double term : orig.terms()) {
            terms.add(Double.doubleToLongBits(term));
        }
        return new DoubleScriptFieldTermsQuery(orig.script(), leafFactory, orig.fieldName(), terms);
    }

    @Override
    protected DoubleScriptFieldTermsQuery mutate(DoubleScriptFieldTermsQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        LongSet terms = new LongHashSet();
        for (double term : orig.terms()) {
            terms.add(Double.doubleToLongBits(term));
        }
        switch (randomInt(2)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                terms = new LongHashSet(terms);
                while (false == terms.add(Double.doubleToLongBits(randomDouble()))) {
                    // Random double was already in the set
                }
                break;
            default:
                fail();
        }
        return new DoubleScriptFieldTermsQuery(script, leafFactory, fieldName, terms);
    }

    @Override
    public void testMatches() {
        DoubleScriptFieldTermsQuery query = new DoubleScriptFieldTermsQuery(
            randomScript(),
            leafFactory,
            "test",
            LongHashSet.from(Double.doubleToLongBits(0.1), Double.doubleToLongBits(0.2), Double.doubleToLongBits(7.5))
        );
        assertTrue(query.matches(new double[] { 0.1 }, 1));
        assertTrue(query.matches(new double[] { 0.2 }, 1));
        assertTrue(query.matches(new double[] { 7.5 }, 1));
        assertTrue(query.matches(new double[] { 0.1, 0 }, 2));
        assertTrue(query.matches(new double[] { 0, 0.1 }, 2));
        assertFalse(query.matches(new double[] { 0 }, 1));
        assertFalse(query.matches(new double[] { 0, 0.1 }, 1));
    }

    @Override
    protected void assertToString(DoubleScriptFieldTermsQuery query) {
        String toString = query.toString(query.fieldName());
        assertThat(toString, startsWith("["));
        assertThat(toString, endsWith("]"));
        List<Double> list = Arrays.asList(toString.substring(1, toString.length() - 1).split(","))
            .stream()
            .map(Double::parseDouble)
            .collect(toList());

        // Assert that the list is sorted
        for (int i = 1; i < list.size(); i++) {
            assertThat(list.get(i), greaterThan(list.get(i - 1)));
        }

        // Assert that all terms are in the list
        for (double term : query.terms()) {
            assertThat(list, hasItem(term));
        }
    }
}
