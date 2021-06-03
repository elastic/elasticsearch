/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DoubleScriptFieldTermQueryTests extends AbstractDoubleScriptFieldQueryTestCase<DoubleScriptFieldTermQuery> {
    @Override
    protected DoubleScriptFieldTermQuery createTestInstance() {
        return new DoubleScriptFieldTermQuery(randomScript(), leafFactory, randomAlphaOfLength(5), randomDouble());
    }

    @Override
    protected DoubleScriptFieldTermQuery copy(DoubleScriptFieldTermQuery orig) {
        return new DoubleScriptFieldTermQuery(orig.script(), leafFactory, orig.fieldName(), orig.term());
    }

    @Override
    protected DoubleScriptFieldTermQuery mutate(DoubleScriptFieldTermQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        double term = orig.term();
        switch (randomInt(2)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                term = randomValueOtherThan(term, ESTestCase::randomDouble);
                break;
            default:
                fail();
        }
        return new DoubleScriptFieldTermQuery(script, leafFactory, fieldName, term);
    }

    @Override
    public void testMatches() {
        DoubleScriptFieldTermQuery query = new DoubleScriptFieldTermQuery(randomScript(), leafFactory, "test", 3.14);
        assertTrue(query.matches(new double[] { 3.14 }, 1));     // Match because value matches
        assertFalse(query.matches(new double[] { 2 }, 1));       // No match because wrong value
        assertFalse(query.matches(new double[] { 2, 3.14 }, 1)); // No match because value after count of values
        assertTrue(query.matches(new double[] { 2, 3.14 }, 2));  // Match because one value matches
    }

    @Override
    protected void assertToString(DoubleScriptFieldTermQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo(Double.toString(query.term())));
    }
}
