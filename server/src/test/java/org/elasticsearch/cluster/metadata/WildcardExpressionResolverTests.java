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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.IndexComponentSelector.ALL_APPLICABLE;
import static org.elasticsearch.action.support.IndexComponentSelector.DATA;
import static org.elasticsearch.action.support.IndexComponentSelector.FAILURES;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFailureStore;
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
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "ku*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("kuku"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "test*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "testX*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("testXXX", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("testXXX", "testXYY", "testYYY", "kuku"))
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
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "testX*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("testXXX", "testXXY", "testXYY"))
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, false, true),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "testX*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("testXYY"))
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, true, false),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "testX*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("testXXX", "testXXY"))
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
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "test*X*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("testXXX", "testXXY", "testXYY"))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "test*X*Y", ALL_APPLICABLE)
            ),
            equalTo(resolvedExpressionsSet("testXXY", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "kuku*Y*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("kukuYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "*Y*", ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet("testXXY", "testXYY", "testYYY", "kukuYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "test*Y*X", ALL_APPLICABLE))
                .size(),
            equalTo(0)
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, "*Y*X", ALL_APPLICABLE))
                .size(),
            equalTo(0)
        );
    }

    public void testAll() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX"))
            .put(indexBuilder("testXYY"))
            .put(indexBuilder("testYYY"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.lenientExpandOpen(),
                SystemIndexAccessLevel.NONE
            );
            assertThat(
                newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, ALL_APPLICABLE)),
                equalTo(resolvedExpressionsSet("testXXX", "testXYY", "testYYY"))
            );
        }

        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.builder(IndicesOptions.lenientExpandOpen())
                    .gatekeeperOptions(
                        IndicesOptions.GatekeeperOptions.builder(IndicesOptions.lenientExpandOpen().gatekeeperOptions())
                            .allowSelectors(false)
                            .build()
                    )
                    .build(),
                SystemIndexAccessLevel.NONE
            );
            assertThat(
                newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, null)),
                equalTo(resolvedExpressionsNoSelectorSet("testXXX", "testXYY", "testYYY"))
            );
        }
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
            assertThat(
                newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, ALL_APPLICABLE)),
                equalTo(newHashSet())
            );
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
                newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, ALL_APPLICABLE)),
                equalTo(resolvedExpressionsSet("index-visible-alias"))
            );
        }
    }

    public void testAllDataStreams() {

        String dataStreamName = "foo_logs";
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        IndexMetadata firstBackingIndexMetadata = createBackingIndex(dataStreamName, 1, epochMillis).build();
        IndexMetadata firstFailureIndexMetadata = createFailureStore(dataStreamName, 1, epochMillis).build();

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
                .put(firstFailureIndexMetadata, true)
                .put(
                    DataStreamTestHelper.newInstance(
                        dataStreamName,
                        List.of(firstBackingIndexMetadata.getIndex()),
                        List.of(firstFailureIndexMetadata.getIndex())
                    )
                );

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
                newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, DATA)),
                equalTo(resolvedExpressionsSet(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis)))
            );
            assertThat(
                newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, ALL_APPLICABLE)),
                equalTo(
                    resolvedExpressionsSet(
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis)
                    )
                )
            );
        }

        {
            // if data stream itself is hidden, backing indices should not be returned
            var dataStream = DataStream.builder(dataStreamName, List.of(firstBackingIndexMetadata.getIndex()))
                .setFailureIndices(
                    DataStream.DataStreamIndices.failureIndicesBuilder(List.of(firstFailureIndexMetadata.getIndex())).build()
                )
                .setHidden(true)
                .build();

            Metadata.Builder mdBuilder = Metadata.builder()
                .put(firstBackingIndexMetadata, true)
                .put(firstFailureIndexMetadata, true)
                .put(dataStream);

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

            assertThat(newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, DATA)), equalTo(Set.of()));
            assertThat(
                newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, ALL_APPLICABLE)),
                equalTo(Set.of())
            );
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
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAndAliasesContext,
                "foo_a*",
                ALL_APPLICABLE
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("foo_index", DATA), new ResolvedExpression("bar_index", DATA)));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                skipAliasesLenientContext,
                "foo_a*",
                ALL_APPLICABLE
            );
            assertEquals(0, indices.size());
        }
        {
            Set<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                skipAliasesStrictContext,
                "foo_a*",
                ALL_APPLICABLE
            );
            assertThat(indices, empty());
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAndAliasesContext,
                "foo*",
                ALL_APPLICABLE
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_foo", DATA),
                    new ResolvedExpression("foo_index", DATA),
                    new ResolvedExpression("bar_index", DATA)
                )
            );
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                skipAliasesLenientContext,
                "foo*",
                ALL_APPLICABLE
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("foo_foo", DATA), new ResolvedExpression("foo_index", DATA)));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                skipAliasesStrictContext,
                "foo*",
                ALL_APPLICABLE
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("foo_foo", DATA), new ResolvedExpression("foo_index", DATA)));
        }
    }

    public void testResolveDataStreams() {
        String dataStreamName = "foo_logs";
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        IndexMetadata firstBackingIndexMetadata = createBackingIndex(dataStreamName, 1, epochMillis).build();
        IndexMetadata secondBackingIndexMetadata = createBackingIndex(dataStreamName, 2, epochMillis).build();
        IndexMetadata firstFailureIndexMetadata = createFailureStore(dataStreamName, 1, epochMillis).build();
        IndexMetadata secondFailureIndexMetadata = createFailureStore(dataStreamName, 2, epochMillis).build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo_foo").state(State.OPEN))
            .put(indexBuilder("bar_bar").state(State.OPEN))
            .put(indexBuilder("foo_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(indexBuilder("bar_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(firstBackingIndexMetadata, true)
            .put(firstFailureIndexMetadata, true)
            .put(secondBackingIndexMetadata, true)
            .put(secondFailureIndexMetadata, true)
            .put(
                DataStreamTestHelper.newInstance(
                    dataStreamName,
                    List.of(firstBackingIndexMetadata.getIndex(), secondBackingIndexMetadata.getIndex()),
                    List.of(firstFailureIndexMetadata.getIndex(), secondFailureIndexMetadata.getIndex())
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
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAndAliasesContext,
                "foo_*",
                ALL_APPLICABLE
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", DATA),
                    new ResolvedExpression("foo_foo", DATA),
                    new ResolvedExpression("bar_index", DATA)
                )
            );

            // data streams are not included and expression doesn't match the data steram
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAndAliasesContext,
                "bar_*",
                ALL_APPLICABLE
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("bar_bar", DATA), new ResolvedExpression("bar_index", DATA)));
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
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesAndDataStreamsContext,
                "foo_*",
                ALL_APPLICABLE
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", DATA),
                    new ResolvedExpression("bar_index", DATA),
                    new ResolvedExpression("foo_foo", DATA),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), DATA),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), DATA),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), DATA),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), DATA)
                )
            );

            // data stream's corresponding failure indices are resolved
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesAndDataStreamsContext,
                "foo_*",
                FAILURES
            );
            assertThat(
                newHashSet(indices),
                equalTo(
                    resolvedExpressionsSet(
                        DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis)
                    )
                )
            );

            // include all wildcard adds the data stream's backing indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesAndDataStreamsContext,
                "*",
                ALL_APPLICABLE
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", DATA),
                    new ResolvedExpression("bar_index", DATA),
                    new ResolvedExpression("foo_foo", DATA),
                    new ResolvedExpression("bar_bar", DATA),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), DATA),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), DATA),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), DATA),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), DATA)
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
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesDataStreamsAndHiddenIndices,
                "foo_*",
                ALL_APPLICABLE
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", DATA),
                    new ResolvedExpression("bar_index", DATA),
                    new ResolvedExpression("foo_foo", DATA),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), DATA),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), DATA),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), DATA),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), DATA)
                )
            );

            // only data stream's corresponding failure indices are resolved
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesDataStreamsAndHiddenIndices,
                "foo_*",
                FAILURES
            );
            assertThat(
                newHashSet(indices),
                equalTo(
                    resolvedExpressionsSet(
                        DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis)
                    )
                )
            );

            // Resolve both backing and failure indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesDataStreamsAndHiddenIndices,
                "foo_*",
                ALL_APPLICABLE
            );
            assertThat(
                newHashSet(indices),
                equalTo(
                    resolvedExpressionsSet(
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis)
                    )
                )
            );

            // include all wildcard adds the data stream's backing indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesDataStreamsAndHiddenIndices,
                "*",
                ALL_APPLICABLE
            );
            assertThat(
                newHashSet(indices),
                equalTo(
                    resolvedExpressionsSet(
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        "bar_bar",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis)
                    )
                )
            );

            // include all wildcard adds the data stream's failure indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesDataStreamsAndHiddenIndices,
                "*",
                FAILURES
            );
            assertThat(
                newHashSet(indices),
                equalTo(
                    resolvedExpressionsSet(
                        DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis)
                    )
                )
            );

            // include all wildcard adds the data stream's backing and failure indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
                indicesAliasesDataStreamsAndHiddenIndices,
                "*",
                ALL_APPLICABLE
            );
            assertThat(
                newHashSet(indices),
                equalTo(
                    resolvedExpressionsSet(
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        "bar_bar",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis)
                    )
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

        Collection<ResolvedExpression> matches = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
            indicesAndAliasesContext,
            "*",
            ALL_APPLICABLE
        );
        assertThat(
            matches,
            containsInAnyOrder(
                new ResolvedExpression("bar_bar", DATA),
                new ResolvedExpression("foo_foo", DATA),
                new ResolvedExpression("foo_index", DATA),
                new ResolvedExpression("bar_index", DATA)
            )
        );
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(onlyIndicesContext, "*", ALL_APPLICABLE);
        assertThat(
            matches,
            containsInAnyOrder(
                new ResolvedExpression("bar_bar", DATA),
                new ResolvedExpression("foo_foo", DATA),
                new ResolvedExpression("foo_index", DATA),
                new ResolvedExpression("bar_index", DATA)
            )
        );
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
            indicesAndAliasesContext,
            "foo*",
            ALL_APPLICABLE
        );
        assertThat(
            matches,
            containsInAnyOrder(
                new ResolvedExpression("foo_foo", DATA),
                new ResolvedExpression("foo_index", DATA),
                new ResolvedExpression("bar_index", DATA)
            )
        );
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(
            onlyIndicesContext,
            "foo*",
            ALL_APPLICABLE
        );
        assertThat(matches, containsInAnyOrder(new ResolvedExpression("foo_foo", DATA), new ResolvedExpression("foo_index", DATA)));
    }

    private static IndexMetadata.Builder indexBuilder(String index, boolean hidden) {
        return IndexMetadata.builder(index)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_HIDDEN_SETTING.getKey(), hidden));
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return indexBuilder(index, false);
    }

    private static void assertWildcardResolvesToEmpty(IndexNameExpressionResolver.Context context, String wildcardExpression) {
        assertThat(
            IndexNameExpressionResolver.WildcardExpressionResolver.matchWildcardToResources(context, wildcardExpression, DATA),
            empty()
        );
    }

    private Set<ResolvedExpression> resolvedExpressionsSet(String... expressions) {
        return Arrays.stream(expressions).map(e -> new ResolvedExpression(e, DATA)).collect(Collectors.toSet());
    }

    private Set<ResolvedExpression> resolvedExpressionsNoSelectorSet(String... expressions) {
        return Arrays.stream(expressions).map(e -> new ResolvedExpression(e)).collect(Collectors.toSet());
    }
}
