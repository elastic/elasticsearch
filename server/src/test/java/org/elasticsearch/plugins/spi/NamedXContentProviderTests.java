/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.spi;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Predicate;

public class NamedXContentProviderTests extends ESTestCase {

    public void testSpiFileExists() throws IOException {
        String serviceFile = "/META-INF/services/" + NamedXContentProvider.class.getName();
        List<String> implementations = new ArrayList<>();
        try (InputStream input = NamedXContentProviderTests.class.getResourceAsStream(serviceFile)) {
            Streams.readAllLines(input, implementations::add);
        }

        assertEquals(1, implementations.size());
        assertEquals(TestNamedXContentProvider.class.getName(), implementations.get(0));
    }

    public void testNamedXContents() {
        final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();
        for (NamedXContentProvider service : ServiceLoader.load(NamedXContentProvider.class)) {
            namedXContents.addAll(service.getNamedXContentParsers());
        }

        assertEquals(2, namedXContents.size());

        List<Predicate<NamedXContentRegistry.Entry>> predicates = new ArrayList<>(2);
        predicates.add(e -> Aggregation.class.equals(e.categoryClass) && "test_aggregation".equals(e.name.getPreferredName()));
        predicates.add(e -> Suggest.Suggestion.class.equals(e.categoryClass) && "test_suggestion".equals(e.name.getPreferredName()));
        predicates.forEach(predicate -> assertEquals(1, namedXContents.stream().filter(predicate).count()));
    }

    public static class TestNamedXContentProvider implements NamedXContentProvider {

        public TestNamedXContentProvider() {}

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
            return Arrays.asList(
                new NamedXContentRegistry.Entry(
                    Aggregation.class,
                    new ParseField("test_aggregation"),
                    (parser, context) -> ParsedSimpleValue.fromXContent(parser, (String) context)
                ),
                new NamedXContentRegistry.Entry(
                    Suggest.Suggestion.class,
                    new ParseField("test_suggestion"),
                    (parser, context) -> TermSuggestion.fromXContent(parser, (String) context)
                )
            );
        }
    }
}
