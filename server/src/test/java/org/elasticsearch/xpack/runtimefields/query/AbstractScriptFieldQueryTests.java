/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.runtimefields.mapper.AbstractFieldScript;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AbstractScriptFieldQueryTests extends ESTestCase {
    public void testExplainMatched() throws IOException {
        AbstractScriptFieldQuery<AbstractFieldScript> query = new AbstractScriptFieldQuery<AbstractFieldScript>(
            new Script("test"),
            "test",
            null
        ) {
            @Override
            protected boolean matches(AbstractFieldScript scriptContext, int docId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String toString(String field) {
                throw new UnsupportedOperationException();
            }
        };
        float boost = randomBoolean() ? 1.0f : randomFloat();
        String dummyDescription = randomAlphaOfLength(10);
        assertThat(
            query.explainMatch(boost, dummyDescription),
            equalTo(
                Explanation.match(
                    boost,
                    dummyDescription,
                    Explanation.match(
                        boost,
                        "boost * runtime_field_score",
                        Explanation.match(boost, "boost"),
                        Explanation.match(1.0, "runtime_field_score is always 1")
                    )
                )
            )
        );
    }
}
