/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.query.IntervalsSourceProvider.IntervalFilter;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilterIntervalsSourceProviderTests extends AbstractXContentSerializingTestCase<IntervalFilter> {

    @Override
    protected IntervalFilter createTestInstance() {
        return IntervalQueryBuilderTests.createRandomNonNullFilter(0, randomBoolean());
    }

    @Override
    protected IntervalFilter mutateInstance(IntervalFilter instance) {
        return mutateFilter(instance);
    }

    static IntervalFilter mutateFilter(IntervalFilter instance) {
        IntervalsSourceProvider filter = instance.getFilter();
        String type = instance.getType();
        Script script = instance.getScript();

        if (filter != null) {
            if (randomBoolean()) {
                if (filter instanceof IntervalsSourceProvider.Match) {
                    filter = WildcardIntervalsSourceProviderTests.createRandomWildcard();
                } else {
                    filter = IntervalQueryBuilderTests.createRandomMatch(0, randomBoolean());
                }
            } else {
                if (type.equals("containing")) {
                    type = "overlapping";
                } else {
                    type = "containing";
                }
            }
            return new IntervalFilter(filter, type);
        } else {
            return new IntervalFilter(new Script(ScriptType.INLINE, "mockscript", script.getIdOrCode() + "foo", Collections.emptyMap()));
        }
    }

    @Override
    protected Writeable.Reader<IntervalFilter> instanceReader() {
        return IntervalFilter::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SearchModule.getIntervalsSourceProviderNamedWritables());
    }

    @Override
    protected IntervalFilter doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        return IntervalFilter.fromXContent(parser);
    }

    public void testCancellationCheckWiredForIntervalFilterScript() throws IOException {
        AtomicReference<IntervalFilterScript> capturedScript = new AtomicReference<>();
        IntervalFilterScript.Factory factory = () -> {
            IntervalFilterScript script = new IntervalFilterScript() {
                @Override
                public boolean execute(IntervalFilterScript.Interval interval) {
                    return true;
                }
            };
            capturedScript.set(script);
            return script;
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
                SearchExecutionContext mockContextWithCis = mock(SearchExecutionContext.class);
                doReturn(factory).when(mockContextWithCis).compile(any(Script.class), same(IntervalFilterScript.CONTEXT));
                when(mockContextWithCis.searcher()).thenReturn(contextSearcher);

                IntervalsSource dummySource = mock(IntervalsSource.class);
                IntervalFilter filter = new IntervalFilter(new Script("test"));
                filter.filter(dummySource, mockContextWithCis, null);
                assertNotNull(capturedScript.get()._getCancellationCheck());

                capturedScript.set(null);
                SearchExecutionContext mockContextPlain = mock(SearchExecutionContext.class);
                doReturn(factory).when(mockContextPlain).compile(any(Script.class), same(IntervalFilterScript.CONTEXT));
                when(mockContextPlain.searcher()).thenReturn(newSearcher(reader));
                filter.filter(dummySource, mockContextPlain, null);
                assertNull(capturedScript.get()._getCancellationCheck());
            }
        }
    }
}
