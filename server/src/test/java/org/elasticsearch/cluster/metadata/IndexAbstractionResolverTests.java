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
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.indices.SystemIndices.EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY;
import static org.elasticsearch.indices.SystemIndices.SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexAbstractionResolverTests extends ESTestCase {

    private IndexNameExpressionResolver indexNameExpressionResolver;
    private IndexAbstractionResolver indexAbstractionResolver;
    private ProjectMetadata projectMetadata;
    private String dateTimeIndexToday;
    private String dateTimeIndexTomorrow;

    // Only used when resolving wildcard expressions
    private final Set<String> defaultMask = Set.of("index1", "index2", "data-stream1");

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Try to resist failing at midnight on the first/last day of the month. Time generally moves forward, so make a timestamp for
        // the next day and if they're different, add both to the cluster state. Check for either in date math tests.
        long timeMillis = System.currentTimeMillis();
        long timeTomorrow = timeMillis + TimeUnit.DAYS.toMillis(1);
        dateTimeIndexToday = IndexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>", timeMillis);
        dateTimeIndexTomorrow = IndexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>", timeTomorrow);

        projectMetadata = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(new Tuple<>("data-stream1", 2), new Tuple<>("data-stream2", 2)),
            List.of("index1", "index2", "index3", dateTimeIndexToday, dateTimeIndexTomorrow),
            randomMillisUpToYear9999(),
            Settings.EMPTY,
            0,
            false,
            true
        );

        indexNameExpressionResolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE,
            TestProjectResolvers.singleProject(projectMetadata.id())
        );
        indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);

    }

    public void testResolveIndexAbstractions() {
        // == Single Concrete Index ==

        // No selectors allowed, none given
        assertThat(resolveAbstractionsSelectorNotAllowed(List.of("index1")), contains("index1"));
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("index1::data")));
        // Selectors allowed, valid selector given, data selector stripped off in result since it is the default
        assertThat(resolveAbstractionsSelectorAllowed(List.of("index1::data")), contains("index1"));
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("index1::*")));

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
            contains(either(equalTo(dateTimeIndexToday)).or(equalTo(dateTimeIndexTomorrow)))
        );
        // Selectors allowed, valid selector provided, data selector stripped off in result since it is the default
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("<datetime-{now/M}>::data")),
            contains(either(equalTo(dateTimeIndexToday)).or(equalTo(dateTimeIndexTomorrow)))
        );
        // Selectors allowed, wildcard selector provided, data selector stripped off in result since it is the default
        // ** only returns ::data since expression is an index
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("<datetime-{now/M}>::data")),
            contains(either(equalTo(dateTimeIndexToday)).or(equalTo(dateTimeIndexTomorrow)))
        );
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("<datetime-{now/M}>::custom")));

        // == Single Patterned Index ==

        // No selectors allowed, none given
        assertThat(resolveAbstractionsSelectorNotAllowed(List.of("index*")), containsInAnyOrder("index1", "index2"));
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("index*::data")));
        // Selectors allowed, valid selector given, data selector stripped off in result since it is the default
        assertThat(resolveAbstractionsSelectorAllowed(List.of("index*::data")), containsInAnyOrder("index1", "index2"));
        // Selectors allowed, wildcard selector provided, data selector stripped off in result since it is the default
        // ** only returns ::data since expression is an index
        assertThat(resolveAbstractionsSelectorAllowed(List.of("index*")), containsInAnyOrder("index1", "index2"));
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("index*::custom")));

        // == Single Data Stream ==

        // No selectors allowed, none given
        assertThat(resolveAbstractionsSelectorNotAllowed(List.of("data-stream1")), contains("data-stream1"));
        // No selectors allowed, valid selector given
        expectThrows(IllegalArgumentException.class, () -> resolveAbstractionsSelectorNotAllowed(List.of("data-stream1::data")));
        // Selectors allowed, valid selector given
        assertThat(resolveAbstractionsSelectorAllowed(List.of("data-stream1::failures")), contains("data-stream1::failures"));
        // Selectors allowed, data selector is not added in result since it is the default
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("data-stream1", "data-stream1::failures")),
            containsInAnyOrder("data-stream1", "data-stream1::failures")
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
        // Selectors allowed, both ::data and ::failures are returned
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("data-stream*", "data-stream*::failures")),
            containsInAnyOrder("data-stream1", "data-stream1::failures")
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
        assertThat(resolveAbstractionsSelectorAllowed(List.of("*::data")), containsInAnyOrder("index1", "index2", "data-stream1"));
        // Selectors allowed, valid selector given
        // ::failures selector returns only data streams, which can have failure components
        assertThat(resolveAbstractionsSelectorAllowed(List.of("*::failures")), contains("data-stream1::failures"));
        // Selectors allowed, wildcard selector provided
        // ** returns both ::data and ::failures for applicable abstractions
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("*", "*::failures")),
            containsInAnyOrder("index1", "index2", "data-stream1", "data-stream1::failures")
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
        assertThat(resolveAbstractionsSelectorAllowed(List.of("*", "*::failures", "-*::data")), contains("data-stream1::failures"));
        // Selectors allowed, wildcard selector provided
        // ** limits the returned values based on selectors
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("*", "*::failures", "-*::failures")),
            containsInAnyOrder("index1", "index2", "data-stream1")
        );
        // Selectors allowed, none given, default to both selectors
        // ** limits the returned values based on selectors
        assertThat(
            resolveAbstractionsSelectorAllowed(List.of("*", "-*::failures")),
            containsInAnyOrder("index1", "index2", "data-stream1")
        );
        // Selectors allowed, invalid selector given
        expectThrows(InvalidIndexNameException.class, () -> resolveAbstractionsSelectorAllowed(List.of("*", "-*::custom")));
    }

    public void testIsIndexVisible() {
        assertThat(isIndexVisible("index1", null), is(true));
        assertThat(isIndexVisible("index1", "data"), is(true));
        assertThat(isIndexVisible("index1", "failures"), is(false)); // *
        // * Indices don't have failure components so the failure component is not visible

        assertThat(isIndexVisible("data-stream1", null), is(true));
        assertThat(isIndexVisible("data-stream1", "data"), is(true));
        assertThat(isIndexVisible("data-stream1", "failures"), is(true));
    }

    public void testIsNetNewSystemIndexVisible() {
        final Settings settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .build();

        final Settings hiddenSettings = Settings.builder().put(settings).put("index.hidden", true).build();

        final IndexMetadata foo = IndexMetadata.builder(".foo").settings(hiddenSettings).system(true).build();
        final IndexMetadata barReindexed = IndexMetadata.builder(".bar-reindexed")
            .settings(hiddenSettings)
            .system(true)
            .putAlias(AliasMetadata.builder(".bar").isHidden(true).build())
            .build();
        final IndexMetadata other = IndexMetadata.builder("other").settings(settings).build();

        final SystemIndexDescriptor fooDescriptor = SystemIndexDescriptor.builder()
            .setDescription("foo indices")
            .setOrigin("foo origin")
            .setPrimaryIndex(".foo")
            .setIndexPattern(".foo*")
            .setSettings(settings)
            .setMappings(mappings())
            .setNetNew()
            .build();
        final SystemIndexDescriptor barDescriptor = SystemIndexDescriptor.builder()
            .setDescription("bar indices")
            .setOrigin("bar origin")
            .setPrimaryIndex(".bar")
            .setIndexPattern(".bar*")
            .setSettings(settings)
            .setMappings(mappings())
            .setNetNew()
            .build();
        final SystemIndices systemIndices = new SystemIndices(
            List.of(new SystemIndices.Feature("name", "description", List.of(fooDescriptor, barDescriptor)))
        );

        projectMetadata = ProjectMetadata.builder(projectMetadata.id()).put(foo, true).put(barReindexed, true).put(other, true).build();

        // these indices options are for the GET _data_streams case
        final IndicesOptions noHiddenNoAliases = IndicesOptions.builder()
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .matchOpen(true)
                    .matchClosed(true)
                    .includeHidden(false)
                    .resolveAliases(false)
                    .build()
            )
            .build();

        {
            final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "true");
            indexNameExpressionResolver = new IndexNameExpressionResolver(
                threadContext,
                systemIndices,
                TestProjectResolvers.singleProject(projectMetadata.id())
            );
            indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);

            // this covers the GET * case -- with system access, you can see everything
            assertThat(isIndexVisible("other", null), is(true));
            assertThat(isIndexVisible(".foo", null), is(true));
            assertThat(isIndexVisible(".bar", null), is(true));

            // but if you don't ask for hidden and aliases, you won't see hidden indices or aliases, naturally
            assertThat(isIndexVisible("other", null, noHiddenNoAliases), is(true));
            assertThat(isIndexVisible(".foo", null, noHiddenNoAliases), is(false));
            assertThat(isIndexVisible(".bar", null, noHiddenNoAliases), is(false));
        }

        {
            final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "false");
            indexNameExpressionResolver = new IndexNameExpressionResolver(
                threadContext,
                systemIndices,
                TestProjectResolvers.DEFAULT_PROJECT_ONLY
            );
            indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);

            // this covers the GET * case -- without system access, you can't see everything
            assertThat(isIndexVisible("other", null), is(true));
            assertThat(isIndexVisible(".foo", null), is(false));
            assertThat(isIndexVisible(".bar", null), is(false));

            // no difference here in the datastream case, you can't see these then, either
            assertThat(isIndexVisible("other", null, noHiddenNoAliases), is(true));
            assertThat(isIndexVisible(".foo", null, noHiddenNoAliases), is(false));
            assertThat(isIndexVisible(".bar", null, noHiddenNoAliases), is(false));
        }

        {
            final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "true");
            threadContext.putHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "some-elastic-product");
            indexNameExpressionResolver = new IndexNameExpressionResolver(
                threadContext,
                systemIndices,
                TestProjectResolvers.singleProject(projectMetadata.id())
            );
            indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);

            // this covers the GET * case -- with product (only) access, you can't see everything
            assertThat(isIndexVisible("other", null), is(true));
            assertThat(isIndexVisible(".foo", null), is(false));
            assertThat(isIndexVisible(".bar", null), is(false));

            // no difference here in the datastream case, you can't see these then, either
            assertThat(isIndexVisible("other", null, noHiddenNoAliases), is(true));
            assertThat(isIndexVisible(".foo", null, noHiddenNoAliases), is(false));
            assertThat(isIndexVisible(".bar", null, noHiddenNoAliases), is(false));
        }
    }

    private static XContentBuilder mappings() {
        try (XContentBuilder builder = jsonBuilder()) {
            return builder.startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field(SystemIndexDescriptor.VERSION_META_KEY, 0)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<String> resolveAbstractionsSelectorNotAllowed(List<String> expressions) {
        return resolveAbstractions(expressions, IndicesOptions.strictExpandHiddenNoSelectors(), defaultMask);
    }

    private List<String> resolveAbstractionsSelectorAllowed(List<String> expressions) {
        return resolveAbstractions(expressions, IndicesOptions.strictExpandOpen(), defaultMask);
    }

    private List<String> resolveAbstractions(List<String> expressions, IndicesOptions indicesOptions, Set<String> mask) {
        return indexAbstractionResolver.resolveIndexAbstractions(
            expressions,
            indicesOptions,
            projectMetadata,
            (ignored) -> mask,
            (ignored, nothing) -> true,
            true
        );
    }

    private boolean isIndexVisible(String index, String selector) {
        return isIndexVisible(index, selector, IndicesOptions.strictExpandHidden());
    }

    private boolean isIndexVisible(String index, String selector, IndicesOptions indicesOptions) {
        return IndexAbstractionResolver.isIndexVisible(
            "*",
            selector,
            index,
            indicesOptions,
            projectMetadata,
            indexNameExpressionResolver,
            true
        );
    }

}
