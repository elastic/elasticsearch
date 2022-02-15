/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;

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
