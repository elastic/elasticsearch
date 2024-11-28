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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
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

        IndicesOptions indicesOptions = randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen());
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            indicesOptions,
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "ku*")),
            equalTo(newHashSet("kuku"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "test*")),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "testX*")),
            equalTo(newHashSet("testXXX", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "*")),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY", "kuku"))
        );
    }

    public void testConvertWildcardsOpenClosedIndicesTests() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX").state(State.OPEN))
            .put(indexBuilder("testXXY").state(State.OPEN))
            .put(indexBuilder("testXYY").state(State.CLOSE))
            .put(indexBuilder("testYYY").state(State.OPEN))
            .put(indexBuilder("testYYX").state(State.CLOSE))
            .put(indexBuilder("kuku").state(State.OPEN));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, true, true),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "testX*")),
            equalTo(newHashSet("testXXX", "testXXY", "testXYY"))
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, false, true),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "testX*")),
            equalTo(newHashSet("testXYY"))
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, true, false),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "testX*")),
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
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "test*X*")),
            equalTo(newHashSet("testXXX", "testXXY", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "test*X*Y")),
            equalTo(newHashSet("testXXY", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "kuku*Y*")),
            equalTo(newHashSet("kukuYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "*Y*")),
            equalTo(newHashSet("testXXY", "testXYY", "testYYY", "kukuYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "test*Y*X")).size(),
            equalTo(0)
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "*Y*X")).size(),
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
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context)),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
    }

    public void testAllAliases() {
        {
            // hidden index with hidden alias should not be returned
            Metadata.Builder mdBuilder = Metadata.builder()
                .put(
                    indexBuilder("index-hidden-alias", true) // index hidden
                        .state(State.OPEN)
                        .putAlias(AliasMetadata.builder("alias-hidden").isHidden(true)) // alias hidden
                );

            ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.lenientExpandOpen(), // don't include hidden
                SystemIndexAccessLevel.NONE
            );
            assertThat(newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context)), equalTo(newHashSet()));
        }

        {
            // hidden index with visible alias should be returned
            Metadata.Builder mdBuilder = Metadata.builder()
                .put(
                    indexBuilder("index-visible-alias", true) // index hidden
                        .state(State.OPEN)
                        .putAlias(AliasMetadata.builder("alias-visible").isHidden(false)) // alias visible
                );

            ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.lenientExpandOpen(), // don't include hidden
                SystemIndexAccessLevel.NONE
            );
            assertThat(
                newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context)),
                equalTo(newHashSet("index-visible-alias"))
            );
        }
    }

    public void testAllDataStreams() {

        String dataStreamName = "foo_logs";
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        IndexMetadata firstBackingIndexMetadata = createBackingIndex(dataStreamName, 1, epochMillis).build();

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

        {
            // visible data streams should be returned by _all even show backing indices are hidden
            Metadata.Builder mdBuilder = Metadata.builder()
                .put(firstBackingIndexMetadata, true)
                .put(DataStreamTestHelper.newInstance(dataStreamName, List.of(firstBackingIndexMetadata.getIndex())));

            ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                indicesAndAliasesOptions,
                false,
                false,
                true,
                SystemIndexAccessLevel.NONE,
                NONE,
                NONE
            );

            assertThat(
                newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context)),
                equalTo(newHashSet(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis)))
            );
        }

        {
            // if data stream itself is hidden, backing indices should not be returned
            var dataStream = DataStream.builder(dataStreamName, List.of(firstBackingIndexMetadata.getIndex())).setHidden(true).build();

            Metadata.Builder mdBuilder = Metadata.builder().put(firstBackingIndexMetadata, true).put(dataStream);

            ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                indicesAndAliasesOptions,
                false,
                false,
                true,
                SystemIndexAccessLevel.NONE,
                NONE,
                NONE
            );

            assertThat(newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context)), equalTo(newHashSet()));
        }
    }

    public void testResolveEmpty() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                indexBuilder("index_open").state(State.OPEN)
                    .putAlias(AliasMetadata.builder("alias_open"))
                    .putAlias(AliasMetadata.builder("alias_hidden").isHidden(true))
            )
            .put(
                indexBuilder("index_closed").state(State.CLOSE)
                    .putAlias(AliasMetadata.builder("alias_closed"))
                    .putAlias(AliasMetadata.builder("alias_hidden").isHidden(true))
            )
            .put(
                indexBuilder("index_hidden_open", true).state(State.OPEN)
                    .putAlias(AliasMetadata.builder("alias_open"))
                    .putAlias(AliasMetadata.builder("alias_hidden").isHidden(true))
            )
            .put(
                indexBuilder("index_hidden_closed", true).state(State.CLOSE)
                    .putAlias(AliasMetadata.builder("alias_closed"))
                    .putAlias(AliasMetadata.builder("alias_hidden").isHidden(true))
            )
            .put(
                indexBuilder(".dot_index_hidden_open", true).state(State.OPEN)
                    .putAlias(AliasMetadata.builder("alias_open"))
                    .putAlias(AliasMetadata.builder("alias_hidden").isHidden(true))
            )
            .put(
                indexBuilder(".dot_index_hidden_closed", true).state(State.CLOSE)
                    .putAlias(AliasMetadata.builder("alias_closed"))
                    .putAlias(AliasMetadata.builder("alias_hidden").isHidden(true))
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndicesOptions onlyOpenIndicesAndAliasesDisallowNoIndicesOption = IndicesOptions.fromOptions(
            randomBoolean(),
            false,
            true,
            false,
            false,
            randomBoolean(),
            randomBoolean(),
            false,
            randomBoolean()
        );
        IndexNameExpressionResolver.Context indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            onlyOpenIndicesAndAliasesDisallowNoIndicesOption,
            randomFrom(SystemIndexAccessLevel.values())
        );
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_closed*");
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_hidden_open*");
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_hidden_closed*");
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, ".dot_index_hidden_closed*");
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "alias_closed*");
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "alias_hidden*");
        IndicesOptions closedAndHiddenIndicesAndAliasesDisallowNoIndicesOption = IndicesOptions.fromOptions(
            randomBoolean(),
            false,
            false,
            true,
            true,
            randomBoolean(),
            randomBoolean(),
            false,
            randomBoolean()
        );
        indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            closedAndHiddenIndicesAndAliasesDisallowNoIndicesOption,
            randomFrom(SystemIndexAccessLevel.values())
        );
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_open*");
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_hidden_open*");
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, ".dot_hidden_open*");
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "alias_open*");
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
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAndAliasesContext,
                "foo_a*"
            );
            assertThat(indices, containsInAnyOrder("foo_index", "bar_index"));
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                skipAliasesLenientContext,
                "foo_a*"
            );
            assertEquals(0, indices.size());
        }
        {
            Set<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                skipAliasesStrictContext,
                "foo_a*"
            );
            assertThat(indices, empty());
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAndAliasesContext,
                "foo*"
            );
            assertThat(indices, containsInAnyOrder("foo_foo", "foo_index", "bar_index"));
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                skipAliasesLenientContext,
                "foo*"
            );
            assertThat(indices, containsInAnyOrder("foo_foo", "foo_index"));
        }
        {
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                skipAliasesStrictContext,
                "foo*"
            );
            assertThat(indices, containsInAnyOrder("foo_foo", "foo_index"));
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
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAndAliasesContext,
                "foo_*"
            );
            assertThat(indices, containsInAnyOrder("foo_index", "foo_foo", "bar_index"));

            // data streams are not included and expression doesn't match the data steram
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(indicesAndAliasesContext, "bar_*");
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
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesAndDataStreamsContext,
                "foo_*"
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
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesAndDataStreamsContext,
                "*"
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
            Collection<String> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesDataStreamsAndHiddenIndices,
                "foo_*"
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
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesDataStreamsAndHiddenIndices,
                "*"
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

        Collection<String> matches = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
            indicesAndAliasesContext,
            "*"
        );
        assertThat(matches, containsInAnyOrder("bar_bar", "foo_foo", "foo_index", "bar_index"));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(onlyIndicesContext, "*");
        assertThat(matches, containsInAnyOrder("bar_bar", "foo_foo", "foo_index", "bar_index"));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(indicesAndAliasesContext, "foo*");
        assertThat(matches, containsInAnyOrder("foo_foo", "foo_index", "bar_index"));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(onlyIndicesContext, "foo*");
        assertThat(matches, containsInAnyOrder("foo_foo", "foo_index"));
    }

    private static IndexMetadata.Builder indexBuilder(String index, boolean hidden) {
        return IndexMetadata.builder(index)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_HIDDEN_SETTING.getKey(), hidden));
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return indexBuilder(index, false);
    }

    private static void assertWildcardResolvesToEmpty(IndexNameExpressionResolver.Context context, String wildcardExpression) {
        assertThat(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, wildcardExpression), empty());
    }
}
