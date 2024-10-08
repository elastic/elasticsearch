/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexAbstractionResolverTests extends ESTestCase {

    private IndexNameExpressionResolver indexNameExpressionResolver;
    private IndexAbstractionResolver indexAbstractionResolver;
    private Metadata metadata;
    private String dateTimeIndexToday;
    private String dateTimeIndexTomorrow;

    // Only used when resolving wildcard expressions
    private final Supplier<Set<String>> defaultMask = () -> Set.of("index1", "index2", "data-stream1");

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexNameExpressionResolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE
        );
        indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);

        // Try to resist failing at midnight on the first/last day of the month. Time generally moves forward, so make a timestamp for
        // the next day and if they're different, add both to the cluster state. Check for either in date math tests.
        long timeMillis = System.currentTimeMillis();
        long timeTomorrow = timeMillis + TimeUnit.DAYS.toMillis(1);
        dateTimeIndexToday = IndexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>", timeMillis);
        dateTimeIndexTomorrow = IndexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>", timeTomorrow);

        metadata = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>("data-stream1", 2), new Tuple<>("data-stream2", 2)),
            List.of("index1", "index2", "index3", dateTimeIndexToday, dateTimeIndexTomorrow),
            randomMillisUpToYear9999(),
            Settings.EMPTY,
            0,
            false,
            true
        ).metadata();
    }

    public void testResolveIndexAbstractions() {
        // == Single Concrete Index ==

        // No selectors allowed, none given
        assertThat(resolveAbstractionsSelectorNotAllowed(List.of("index1")), contains("index1"));
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("index1::data")));
        // Selectors allowed, valid selector given
        assertThat(resolveAbstractionsSelectorAllowed(List.of("index1::data")), contains("index1::data"));
        // Selectors allowed, wildcard selector provided
        // ** only returns ::data since expression is an index
        assertThat(resolveAbstractionsSelectorAllowed(List.of("index1::*")), contains("index1::data"));
        // Selectors allowed, none given, default to both selectors
        // ** only returns ::data since expression is an index
        assertThat(resolveAbstractionsDefaultBothSelector(List.of("index1::*")), contains("index1::data"));
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("index1::custom")));

        // == Single Date Math Expressions ==

        // No selectors allowed, none given
        assertThat(
            resolveAbstractionsSelectorNotAllowed(List.of("<datetime-{now/M}>")),
            contains(either(equalTo(dateTimeIndexToday)).or(equalTo(dateTimeIndexTomorrow)))
        );
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("<datetime-{now/M}>::data")));
        // Selectors allowed, none given
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("<datetime-{now/M}>")),
            contains(either(equalTo(dateTimeIndexToday + "::data")).or(equalTo(dateTimeIndexTomorrow + "::data")))
        );
        // Selectors allowed, valid selector provided
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("<datetime-{now/M}>::data")),
            contains(either(equalTo(dateTimeIndexToday + "::data")).or(equalTo(dateTimeIndexTomorrow + "::data")))
        );
        // Selectors allowed, wildcard selector provided
        // ** only returns ::data since expression is an index
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("<datetime-{now/M}>::data")),
            contains(either(equalTo(dateTimeIndexToday + "::data")).or(equalTo(dateTimeIndexTomorrow + "::data")))
        );
        // Selectors allowed, none given, default to both selectors
        // ** only returns ::data since expression is an index
        assertThat(
            resolveAbstractionsDefaultBothSelector(List.of("<datetime-{now/M}>::data")),
            contains(either(equalTo(dateTimeIndexToday + "::data")).or(equalTo(dateTimeIndexTomorrow + "::data")))
        );
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("<datetime-{now/M}>::custom")));

        // == Single Patterned Index ==

        // No selectors allowed, none given
        assertThat(resolveAbstractionsSelectorNotAllowed(List.of("index*")), containsInAnyOrder("index1", "index2"));
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("index*::data")));
        // Selectors allowed, valid selector given
        assertThat(resolveAbstractionsSelectorAllowed(List.of("index*::data")), containsInAnyOrder("index1::data", "index2::data"));
        // Selectors allowed, wildcard selector provided
        // ** only returns ::data since expression is an index
        assertThat(resolveAbstractionsSelectorAllowed(List.of("index*::*")), containsInAnyOrder("index1::data", "index2::data"));
        // Selectors allowed, none given, default to both selectors
        // ** only returns ::data since expression is an index
        assertThat(resolveAbstractionsDefaultBothSelector(List.of("index*::*")), containsInAnyOrder("index1::data", "index2::data"));
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("index*::custom")));

        // == Single Data Stream ==

        // No selectors allowed, none given
        assertThat(resolveAbstractionsSelectorNotAllowed(List.of("data-stream1")), contains("data-stream1"));
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("data-stream1::data")));
        // Selectors allowed, valid selector given
        assertThat(resolveAbstractionsSelectorAllowed(List.of("data-stream1::failures")), contains("data-stream1::failures"));
        // Selectors allowed, wildcard selector provided
        // ** returns both ::data and ::failures since expression is a data stream
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("data-stream1::*")),
            containsInAnyOrder("data-stream1::data", "data-stream1::failures")
        );
        // Selectors allowed, none given, default to both selectors
        // ** returns both ::data and ::failures since expression is a data stream
        assertThat(
            resolveAbstractionsDefaultBothSelector(List.of("data-stream1::*")),
            containsInAnyOrder("data-stream1::data", "data-stream1::failures")
        );
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("data-stream1::custom")));

        // == Patterned Data Stream ==

        // No selectors allowed, none given
        assertThat(resolveAbstractionsSelectorNotAllowed(List.of("data-stream*")), contains("data-stream1"));
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("data-stream*::data")));
        // Selectors allowed, valid selector given
        assertThat(resolveAbstractionsSelectorAllowed(List.of("data-stream*::failures")), contains("data-stream1::failures"));
        // Selectors allowed, wildcard selector provided
        // ** returns both ::data and ::failures since expression is a data stream
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("data-stream*::*")),
            containsInAnyOrder("data-stream1::data", "data-stream1::failures")
        );
        // Selectors allowed, none given, default to both selectors
        // ** returns both ::data and ::failures since expression is a data stream
        assertThat(
            resolveAbstractionsDefaultBothSelector(List.of("data-stream*::*")),
            containsInAnyOrder("data-stream1::data", "data-stream1::failures")
        );
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("data-stream*::custom")));

        // == Match All * Wildcard ==

        // No selectors allowed, none given
        assertThat(resolveAbstractionsSelectorNotAllowed(List.of("*")), containsInAnyOrder("index1", "index2", "data-stream1"));
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("*::data")));
        // Selectors allowed, valid selector given
        // ::data selector returns all values with data component
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("*::data")),
            containsInAnyOrder("index1::data", "index2::data", "data-stream1::data")
        );
        // Selectors allowed, valid selector given
        // ::failures selector returns only data streams, which can have failure components
        assertThat(resolveAbstractionsSelectorAllowed(List.of("*::failures")), contains("data-stream1::failures"));
        // Selectors allowed, wildcard selector provided
        // ** returns both ::data and ::failures for applicable abstractions
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("*::*")),
            containsInAnyOrder("index1::data", "index2::data", "data-stream1::data", "data-stream1::failures")
        );
        // Selectors allowed, none given, default to both selectors
        // ** returns both ::data and ::failures for applicable abstractions
        assertThat(
            resolveAbstractionsDefaultBothSelector(List.of("*")),
            containsInAnyOrder("index1::data", "index2::data", "data-stream1::data", "data-stream1::failures")
        );
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("*::custom")));

        // == Wildcards with Exclusions ==

        // No selectors allowed, none given
        assertThat(resolveAbstractionsSelectorNotAllowed(List.of("*", "-index*")), containsInAnyOrder("data-stream1"));
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("*", "-*::data")));
        // Selectors allowed, wildcard selector provided
        // ** returns both ::data and ::failures for applicable abstractions
        // ** limits the returned values based on selectors
        assertThat(resolveAbstractionsSelectorAllowed(List.of("*::*", "-*::data")), contains("data-stream1::failures"));
        // Selectors allowed, wildcard selector provided
        // ** limits the returned values based on selectors
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("*::*", "-*::failures")),
            containsInAnyOrder("index1::data", "index2::data", "data-stream1::data")
        );
        // Selectors allowed, none given, default to both selectors
        // ** limits the returned values based on selectors
        assertThat(resolveAbstractionsDefaultBothSelector(List.of("*", "-*::data")), containsInAnyOrder("data-stream1::failures"));
        // Selectors allowed, none given, default to both selectors
        // ** limits the returned values based on selectors
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("*", "-*::failures")),
            containsInAnyOrder("index1::data", "index2::data", "data-stream1::data")
        );
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("*", "-*::custom")));
    }

    public void testIsIndexVisible() {
        assertThat(isIndexVisible("index1", null), is(true));
        assertThat(isIndexVisible("index1", "*"), is(true));
        assertThat(isIndexVisible("index1", "data"), is(true));
        assertThat(isIndexVisible("index1", "failures"), is(false)); // *
        // * Indices don't have failure components so the failure component is not visible

        assertThat(isIndexVisible("data-stream1", null), is(true));
        assertThat(isIndexVisible("data-stream1", "*"), is(true));
        assertThat(isIndexVisible("data-stream1", "data"), is(true));
        assertThat(isIndexVisible("data-stream1", "failures"), is(true));
    }

    private boolean isIndexVisible(String index, String selector) {
        return IndexAbstractionResolver.isIndexVisible(
            "*",
            selector,
            index,
            IndicesOptions.strictExpandOpen(),
            metadata,
            indexNameExpressionResolver,
            true
        );
    }

    private List<String> resolveAbstractionsSelectorNotAllowed(List<String> expressions) {
        return resolveAbstractions(expressions, IndicesOptions.strictExpandHiddenNoSelectors(), defaultMask);
    }

    private List<String> resolveAbstractionsSelectorAllowed(List<String> expressions) {
        return resolveAbstractions(expressions, IndicesOptions.strictExpandOpen(), defaultMask);
    }

    private List<String> resolveAbstractionsDefaultBothSelector(List<String> expressions) {
        return resolveAbstractions(expressions, IndicesOptions.strictExpandOpenIncludeFailureStore(), defaultMask);
    }

    private List<String> resolveAbstractions(List<String> expressions, IndicesOptions indicesOptions, Supplier<Set<String>> mask) {
        return indexAbstractionResolver.resolveIndexAbstractions(expressions, indicesOptions, metadata, mask, (idx) -> true, true);
    }
}
