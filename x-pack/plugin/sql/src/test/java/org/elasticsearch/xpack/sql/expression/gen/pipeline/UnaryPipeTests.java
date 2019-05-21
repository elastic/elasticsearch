/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.UnaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipesTests.DummyPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe.AttributeResolver;
import org.elasticsearch.xpack.sql.tree.Source;

import static org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipesTests.tracksScores;

public class UnaryPipeTests extends ESTestCase {
    public void testSupportedByAggsOnlyQuery() {
        Pipe supported = new DummyPipe(true);
        Pipe unsupported = new DummyPipe(false);

        assertFalse(newUnaryProcessor(unsupported).supportedByAggsOnlyQuery());
        assertTrue(newUnaryProcessor(supported).supportedByAggsOnlyQuery());
    }

    public void testResolveAttributes() {
        Pipe needsNothing = new DummyPipe(randomBoolean());
        Pipe resolvesTo = new DummyPipe(randomBoolean());
        Pipe needsResolution = new DummyPipe(randomBoolean()) {
            @Override
            public Pipe resolveAttributes(AttributeResolver resolver) {
                return resolvesTo;
            }
        };
        AttributeResolver resolver = a -> {
            fail("not exepected");
            return null;
        };

        Pipe d = newUnaryProcessor(needsNothing);
        assertSame(d, d.resolveAttributes(resolver));

        d = newUnaryProcessor(needsResolution);
        Pipe expected = newUnaryProcessor(resolvesTo);
        assertEquals(expected, d.resolveAttributes(resolver));
    }

    public void testCollectFields() {
        DummyPipe wantsScore = new DummyPipe(randomBoolean()) {
            @Override
            public void collectFields(SqlSourceBuilder sourceBuilder) {
                sourceBuilder.trackScores();
            }
        };
        DummyPipe wantsNothing = new DummyPipe(randomBoolean());
        assertFalse(tracksScores(newUnaryProcessor(wantsNothing)));
        assertTrue(tracksScores(newUnaryProcessor(wantsScore)));
    }

    private Pipe newUnaryProcessor(Pipe child) {
        return new UnaryPipe(Source.EMPTY, null, child, null);
    }
}
