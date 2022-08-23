/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class WildcardExpressionResolverTests extends ESTestCase {

    private static final Predicate<String> NONE = name -> false;

    public void testConvertWildcardsJustIndicesTests() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX"))
            .put(indexBuilder("testXYY"))
            .put(indexBuilder("testYYY"))
            .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("testXXX"))),
            equalTo(newHashSet("testXXX"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("testXXX", "testYYY"))),
            equalTo(newHashSet("testXXX", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("testXXX", "ku*"))),
            equalTo(newHashSet("testXXX", "kuku"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("test*"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("testX*"))),
            equalTo(newHashSet("testXXX", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("testX*", "kuku"))),
            equalTo(newHashSet("testXXX", "testXYY", "kuku"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("*"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY", "kuku"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("*", "-kuku"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("testXXX", "-testXXX"))),
            equalTo(newHashSet("testXXX", "-testXXX"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("testXXX", "-testX*"))),
            equalTo(newHashSet("testXXX"))
        );
    }

    public void testConvertWildcardsTests() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX").putAlias(AliasMetadata.builder("alias1")).putAlias(AliasMetadata.builder("alias2")))
            .put(indexBuilder("testXYY").putAlias(AliasMetadata.builder("alias2")))
            .put(indexBuilder("testYYY").putAlias(AliasMetadata.builder("alias3")))
            .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("testYY*", "alias*"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("-kuku"))),
            equalTo(newHashSet("-kuku"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("test*", "-testYYY"))),
            equalTo(newHashSet("testXXX", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("testX*", "testYYY"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Arrays.asList("testYYY", "testX*"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
    }

    public void testConvertWildcardsOpenClosedIndicesTests() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX").state(IndexMetadata.State.OPEN))
            .put(indexBuilder("testXXY").state(IndexMetadata.State.OPEN))
            .put(indexBuilder("testXYY").state(IndexMetadata.State.CLOSE))
            .put(indexBuilder("testYYY").state(IndexMetadata.State.OPEN))
            .put(indexBuilder("testYYX").state(IndexMetadata.State.CLOSE))
            .put(indexBuilder("kuku").state(IndexMetadata.State.OPEN));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, true, true),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("testX*"))),
            equalTo(newHashSet("testXXX", "testXXY", "testXYY"))
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, false, true),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("testX*"))),
            equalTo(newHashSet("testXYY"))
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, true, false),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("testX*"))),
            equalTo(newHashSet("testXXX", "testXXY"))
        );
    }

    // issue #13334
    public void testMultipleWildcards() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX"))
            .put(indexBuilder("testXXY"))
            .put(indexBuilder("testXYY"))
            .put(indexBuilder("testYYY"))
            .put(indexBuilder("kuku"))
            .put(indexBuilder("kukuYYY"));

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("test*X*"))),
            equalTo(newHashSet("testXXX", "testXXY", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("test*X*Y"))),
            equalTo(newHashSet("testXXY", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("kuku*Y*"))),
            equalTo(newHashSet("kukuYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("*Y*"))),
            equalTo(newHashSet("testXXY", "testXYY", "testYYY", "kukuYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("test*Y*X")))
                .size(),
            equalTo(0)
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("*Y*X"))).size(),
            equalTo(0)
        );
    }

    public void testAll() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX"))
            .put(indexBuilder("testXYY"))
            .put(indexBuilder("testYYY"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, Collections.singletonList("_all"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
    }

    public void testResolveAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo_foo").state(State.OPEN))
            .put(indexBuilder("bar_bar").state(State.OPEN))
            .put(indexBuilder("foo_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(indexBuilder("bar_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        // when ignoreAliases option is not set, WildcardExpressionResolver resolves the provided
        // expressions against the defined indices and aliases
        IndicesOptions indicesAndAliasesOptions = IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            true,
            false,
            true,
            false,
            false,
            false
        );
        IndexNameExpressionResolver.Context indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            indicesAndAliasesOptions,
            SystemIndexAccessLevel.NONE
        );
        // ignoreAliases option is set, WildcardExpressionResolver throws error when
        IndicesOptions skipAliasesIndicesOptions = IndicesOptions.fromOptions(true, true, true, false, true, false, true, false);
        IndexNameExpressionResolver.Context skipAliasesLenientContext = new IndexNameExpressionResolver.Context(
            state,
            skipAliasesIndicesOptions,
            SystemIndexAccessLevel.NONE
        );
        // ignoreAliases option is set, WildcardExpressionResolver resolves the provided expressions only against the defined indices
        IndicesOptions errorOnAliasIndicesOptions = IndicesOptions.fromOptions(false, false, true, false, true, false, true, false);
        IndexNameExpressionResolver.Context skipAliasesStrictContext = new IndexNameExpressionResolver.Context(
            state,
            errorOnAliasIndicesOptions,
            SystemIndexAccessLevel.NONE
        );

        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                Collections.singletonList("foo_a*")
            );
            assertThat(indices, containsInAnyOrder("foo_index", "bar_index"));
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesLenientContext,
                Collections.singletonList("foo_a*")
            );
            assertEquals(0, indices.size());
        }
        {
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    skipAliasesStrictContext,
                    Collections.singletonList("foo_a*")
                )
            );
            assertEquals("foo_a*", infe.getIndex().getName());
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                Collections.singletonList("foo*")
            );
            assertThat(indices, containsInAnyOrder("foo_foo", "foo_index", "bar_index"));
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesLenientContext,
                Collections.singletonList("foo*")
            );
            assertThat(indices, containsInAnyOrder("foo_foo", "foo_index"));
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesStrictContext,
                Collections.singletonList("foo*")
            );
            assertThat(indices, containsInAnyOrder("foo_foo", "foo_index"));
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                Collections.singletonList("foo_alias")
            );
            assertThat(indices, containsInAnyOrder("foo_alias"));
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesLenientContext,
                Collections.singletonList("foo_alias")
            );
            assertThat(indices, containsInAnyOrder("foo_alias"));
        }
        {
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    skipAliasesStrictContext,
                    Collections.singletonList("foo_alias")
                )
            );
            assertEquals(
                "The provided expression [foo_alias] matches an alias, " + "specify the corresponding concrete indices instead.",
                iae.getMessage()
            );
        }
    }

    public void testResolveDataStreams() {
        String dataStreamName = "foo_logs";
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        IndexMetadata firstBackingIndexMetadata = createBackingIndex(dataStreamName, 1, epochMillis).build();
        IndexMetadata secondBackingIndexMetadata = createBackingIndex(dataStreamName, 2, epochMillis).build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo_foo").state(State.OPEN))
            .put(indexBuilder("bar_bar").state(State.OPEN))
            .put(indexBuilder("foo_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(indexBuilder("bar_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(firstBackingIndexMetadata, true)
            .put(secondBackingIndexMetadata, true)
            .put(
                DataStreamTestHelper.newInstance(
                    dataStreamName,
                    List.of(firstBackingIndexMetadata.getIndex(), secondBackingIndexMetadata.getIndex())
                )
            );

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        {
            IndicesOptions indicesAndAliasesOptions = IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                false,
                true,
                false,
                false,
                false
            );
            IndexNameExpressionResolver.Context indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
                state,
                indicesAndAliasesOptions,
                SystemIndexAccessLevel.NONE
            );

            // data streams are not included but expression matches the data stream
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                Collections.singletonList("foo_*")
            );
            assertThat(indices, containsInAnyOrder("foo_index", "foo_foo", "bar_index"));

            // data streams are not included and expression doesn't match the data steram
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                Collections.singletonList("bar_*")
            );
            assertThat(indices, containsInAnyOrder("bar_bar", "bar_index"));
        }

        {
            IndicesOptions indicesAndAliasesOptions = IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                false,
                true,
                false,
                false,
                false
            );
            IndexNameExpressionResolver.Context indicesAliasesAndDataStreamsContext = new IndexNameExpressionResolver.Context(
                state,
                indicesAndAliasesOptions,
                false,
                false,
                true,
                SystemIndexAccessLevel.NONE,
                NONE,
                NONE
            );

            // data stream's corresponding backing indices are resolved
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesAndDataStreamsContext,
                Collections.singletonList("foo_*")
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    "foo_index",
                    "bar_index",
                    "foo_foo",
                    DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                    DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis)
                )
            );

            // include all wildcard adds the data stream's backing indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesAndDataStreamsContext,
                Collections.singletonList("*")
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    "foo_index",
                    "bar_index",
                    "foo_foo",
                    "bar_bar",
                    DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                    DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis)
                )
            );
        }

        {
            IndicesOptions indicesAliasesAndExpandHiddenOptions = IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                false,
                true,
                true,
                false,
                false,
                false
            );
            IndexNameExpressionResolver.Context indicesAliasesDataStreamsAndHiddenIndices = new IndexNameExpressionResolver.Context(
                state,
                indicesAliasesAndExpandHiddenOptions,
                false,
                false,
                true,
                SystemIndexAccessLevel.NONE,
                NONE,
                NONE
            );

            // data stream's corresponding backing indices are resolved
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                Collections.singletonList("foo_*")
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    "foo_index",
                    "bar_index",
                    "foo_foo",
                    DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                    DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis)
                )
            );

            // include all wildcard adds the data stream's backing indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                Collections.singletonList("*")
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    "foo_index",
                    "bar_index",
                    "foo_foo",
                    "bar_bar",
                    DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                    DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis)
                )
            );
        }
    }

    public void testMatchesConcreteIndicesWildcardAndAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo_foo").state(State.OPEN))
            .put(indexBuilder("bar_bar").state(State.OPEN))
            .put(indexBuilder("foo_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(indexBuilder("bar_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        // when ignoreAliases option is not set, WildcardExpressionResolver resolves the provided
        // expressions against the defined indices and aliases
        IndicesOptions indicesAndAliasesOptions = IndicesOptions.fromOptions(false, false, true, false, true, false, false, false);
        IndexNameExpressionResolver.Context indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            indicesAndAliasesOptions,
            SystemIndexAccessLevel.NONE
        );

        // ignoreAliases option is set, WildcardExpressionResolver resolves the provided expressions
        // only against the defined indices
        IndicesOptions onlyIndicesOptions = IndicesOptions.fromOptions(false, false, true, false, true, false, true, false);
        IndexNameExpressionResolver.Context onlyIndicesContext = new IndexNameExpressionResolver.Context(
            state,
            onlyIndicesOptions,
            SystemIndexAccessLevel.NONE
        );

        Collection<String> matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(indicesAndAliasesContext, List.of("*"));
        assertThat(matches, containsInAnyOrder("bar_bar", "foo_foo", "foo_index", "bar_index"));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(onlyIndicesContext, List.of("*"));
        assertThat(matches, containsInAnyOrder("bar_bar", "foo_foo", "foo_index", "bar_index"));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(indicesAndAliasesContext, List.of("foo*"));
        assertThat(matches, containsInAnyOrder("foo_foo", "foo_index", "bar_index"));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(onlyIndicesContext, List.of("foo*"));
        assertThat(matches, containsInAnyOrder("foo_foo", "foo_index"));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(indicesAndAliasesContext, List.of("foo_alias"));
        assertThat(matches, containsInAnyOrder("foo_alias"));
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexNameExpressionResolver.WildcardExpressionResolver.resolve(onlyIndicesContext, List.of("foo_alias"))
        );
        assertThat(
            iae.getMessage(),
            containsString("The provided expression [foo_alias] matches an alias, specify the corresponding " + "concrete indices instead")
        );
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return IndexMetadata.builder(index)
            .settings(
                settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
    }
}
