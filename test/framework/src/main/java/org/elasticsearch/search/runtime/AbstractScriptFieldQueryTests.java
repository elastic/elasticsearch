/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

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
            AbstractScriptFieldQuery.explainMatch(boost, dummyDescription),
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

    public void testCancellationCheckWiredFromContextIndexSearcher() throws IOException {
        AtomicReference<AbstractFieldScript> capturedScript = new AtomicReference<>();
        Function<Object, AbstractFieldScript> scriptFactory = ignored -> {
            AbstractFieldScript script = mock(AbstractFieldScript.class);
            capturedScript.set(script);
            return script;
        };
        AbstractScriptFieldQuery<AbstractFieldScript> query = new AbstractScriptFieldQuery<AbstractFieldScript>(
            new Script("test"),
            "field",
            ctx -> scriptFactory.apply(ctx)
        ) {
            @Override
            protected boolean matches(AbstractFieldScript scriptContext, int docId) {
                return false;
            }

            @Override
            public String toString(String field) {
                return "test";
            }
        };

        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(new Document());
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher contextSearcher = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true
                );
                query.createWeight(contextSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f).scorerSupplier(reader.leaves().get(0));
                AbstractFieldScript scriptWithContext = capturedScript.get();
                assertNotNull(scriptWithContext);
                org.mockito.Mockito.verify(scriptWithContext)._setCancellationCheck(org.mockito.ArgumentMatchers.notNull());

                capturedScript.set(null);
                IndexSearcher plainSearcher = newSearcher(reader);
                query.createWeight(plainSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f).scorerSupplier(reader.leaves().get(0));
                AbstractFieldScript scriptWithoutContext = capturedScript.get();
                assertNotNull(scriptWithoutContext);
                org.mockito.Mockito.verify(scriptWithoutContext)._setCancellationCheck(org.mockito.ArgumentMatchers.isNull());
            }
        }
    }
}
