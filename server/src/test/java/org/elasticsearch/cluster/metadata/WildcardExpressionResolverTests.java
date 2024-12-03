/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.WildcardExpressionResolver;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFirstFailureStore;
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
        IndexComponentSelector selector = randomFrom(IndexComponentSelector.ALL_APPLICABLE, IndexComponentSelector.DATA);
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "ku*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("kuku")));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "test*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("testXXX", "testXYY", "testYYY")));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "testX*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("testXXX", "testXYY")));
            assertThat(failures, empty());

            // test non-existing exclusion
            WildcardExpressionResolver.processWildcards(context, "bla*", selector, data, failures, true);
            assertThat(data, equalTo(Set.of("testXXX", "testXYY")));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("testXXX", "testXYY", "testYYY", "kuku")));
            assertThat(failures, empty());

            // test exclusion
            WildcardExpressionResolver.processWildcards(context, "kuk*", selector, data, failures, true);
            assertThat(data, equalTo(Set.of("testXXX", "testXYY", "testYYY")));
            assertThat(failures, empty());
        }

        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "testX*", IndexComponentSelector.FAILURES, data, failures, false);
            assertThat(data, empty());
            assertThat(failures, empty());
        }
    }

    public void testThrowingErrorWithExpressionsThatDoNotExpand() {
        Metadata.Builder mdBuilder = Metadata.builder().put(indexBuilder("my-index"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        {
            IndexNameExpressionResolver.Context strictContext = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.builder().wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(false)).build(),
                SystemIndexAccessLevel.NONE
            );
            {
                IndexNotFoundException infe = expectThrows(IndexNotFoundException.class, () -> {
                    Set<String> data = new HashSet<>();
                    Set<String> failures = new HashSet<>();
                    WildcardExpressionResolver.processWildcards(
                        strictContext,
                        "does-not-exist*",
                        IndexComponentSelector.DATA,
                        data,
                        failures,
                        randomBoolean()
                    );
                });
                assertEquals("does-not-exist*::data", infe.getIndex().getName());
            }

            {
                IndexNotFoundException infe = expectThrows(IndexNotFoundException.class, () -> {
                    Set<String> data = new HashSet<>();
                    Set<String> failures = new HashSet<>();
                    WildcardExpressionResolver.processWildcards(
                        strictContext,
                        "does*not-exist*",
                        IndexComponentSelector.DATA,
                        data,
                        failures,
                        randomBoolean()
                    );
                });
                assertEquals("does*not-exist*::data", infe.getIndex().getName());
            }
            {
                IndexNotFoundException infe = expectThrows(IndexNotFoundException.class, () -> {
                    Set<String> data = new HashSet<>();
                    Set<String> failures = new HashSet<>();
                    WildcardExpressionResolver.processWildcards(strictContext, "*", IndexComponentSelector.FAILURES, data, failures, false);
                });
                assertEquals("*::failures", infe.getIndex().getName());
            }
            {
                IndexNotFoundException infe = expectThrows(IndexNotFoundException.class, () -> {
                    Set<String> data = new HashSet<>();
                    Set<String> failures = new HashSet<>();
                    WildcardExpressionResolver.processWildcards(
                        strictContext,
                        "testX*",
                        IndexComponentSelector.FAILURES,
                        data,
                        failures,
                        false
                    );
                });
                assertEquals("testX*::failures", infe.getIndex().getName());
            }
            {
                IndexNotFoundException infe = expectThrows(IndexNotFoundException.class, () -> {
                    Set<String> data = new HashSet<>();
                    Set<String> failures = new HashSet<>();
                    WildcardExpressionResolver.processWildcards(
                        strictContext,
                        "t*stX*",
                        IndexComponentSelector.FAILURES,
                        data,
                        failures,
                        false
                    );
                });
                assertEquals("t*stX*::failures", infe.getIndex().getName());
            }
        }
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

        IndexComponentSelector selector = randomFrom(IndexComponentSelector.ALL_APPLICABLE, IndexComponentSelector.DATA);
        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.fromOptions(true, true, true, true),
                SystemIndexAccessLevel.NONE
            );
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "testX*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("testXXX", "testXXY", "testXYY")));
            assertThat(failures, empty());
        }
        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.fromOptions(true, true, false, true),
                SystemIndexAccessLevel.NONE
            );
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "testX*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("testXYY")));
            assertThat(failures, empty());
        }
        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.fromOptions(true, true, true, false),
                SystemIndexAccessLevel.NONE
            );
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "testX*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("testXXX", "testXXY")));
            assertThat(failures, empty());
        }
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
        IndexComponentSelector selector = randomFrom(IndexComponentSelector.ALL_APPLICABLE, IndexComponentSelector.DATA);
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "test*X*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("testXXX", "testXXY", "testXYY")));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "test*X*Y", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("testXXY", "testXYY")));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "kuku*Y*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("kukuYYY")));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "*Y*", selector, data, failures, false);
            assertThat(data, equalTo(Set.of("testXXY", "testXYY", "testYYY", "kukuYYY")));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "test*Y*X", selector, data, failures, false);
            assertThat(data, empty());
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(context, "*Y*X", selector, data, failures, false);
            assertThat(data, empty());
            assertThat(failures, empty());
        }
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
            IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context),
            equalTo(Set.of("testXXX", "testXYY", "testYYY"))
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
            assertThat(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context), empty());
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
            assertThat(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context), equalTo(Set.of("index-visible-alias")));
        }
    }

    public void testAllDataStreams() {
        // TODO-PR see how the selector will combine with the resolveAll.
        String dataStreamName = "foo_logs";
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        IndexMetadata firstBackingIndexMetadata = createBackingIndex(dataStreamName, 1, epochMillis).build();
        IndexMetadata firstFailureIndexMetadata = createFirstFailureStore(dataStreamName, epochMillis).build();

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
                WildcardExpressionResolver.resolveAll(context),
                equalTo(Set.of(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis)))
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

            assertThat(WildcardExpressionResolver.resolveAll(context), empty());
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

        IndexComponentSelector selector = randomFrom(IndexComponentSelector.ALL_APPLICABLE, IndexComponentSelector.DATA);

        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(indicesAndAliasesContext, "foo_a*", selector, data, failures, false);
            assertThat(data, containsInAnyOrder("foo_index", "bar_index"));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(skipAliasesLenientContext, "foo_a*", selector, data, failures, false);
            assertThat(data, empty());
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> WildcardExpressionResolver.processWildcards(skipAliasesStrictContext, "foo_a*", selector, data, failures, false)
            );
            assertThat(infe.getIndex().getName(), equalTo("foo_a*::" + selector.getKey()));
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(indicesAndAliasesContext, "foo*", selector, data, failures, false);
            assertThat(data, containsInAnyOrder("foo_foo", "foo_index", "bar_index"));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(skipAliasesLenientContext, "foo*", selector, data, failures, false);
            assertThat(data, containsInAnyOrder("foo_foo", "foo_index"));
            assertThat(failures, empty());
        }
        {
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(skipAliasesStrictContext, "foo*", selector, data, failures, false);
            assertThat(data, containsInAnyOrder("foo_foo", "foo_index"));
            assertThat(failures, empty());
        }
    }

    public void testResolveDataStreams() {
        String dataStreamName = "foo_logs";
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        IndexMetadata firstBackingIndexMetadata = createBackingIndex(dataStreamName, 1, epochMillis).build();
        IndexMetadata secondBackingIndexMetadata = createBackingIndex(dataStreamName, 2, epochMillis).build();
        IndexMetadata firstFailureIndexMetadata = createFirstFailureStore(dataStreamName, epochMillis).build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo_foo").state(State.OPEN))
            .put(indexBuilder("bar_bar").state(State.OPEN))
            .put(indexBuilder("foo_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(indexBuilder("bar_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(firstBackingIndexMetadata, true)
            .put(secondBackingIndexMetadata, true)
            .put(firstFailureIndexMetadata, true)
            .put(
                DataStreamTestHelper.newInstance(
                    dataStreamName,
                    List.of(firstBackingIndexMetadata.getIndex(), secondBackingIndexMetadata.getIndex()),
                    List.of(firstFailureIndexMetadata.getIndex())
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
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                indicesAndAliasesContext,
                "foo_*",
                randomFrom(IndexComponentSelector.DATA, IndexComponentSelector.ALL_APPLICABLE),
                data,
                failures,
                false
            );
            assertThat(data, equalTo(Set.of("foo_index", "foo_foo", "bar_index")));
            assertThat(failures, empty());

            // data streams are not included and expression doesn't match the data stream
            data = new HashSet<>();
            failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                indicesAndAliasesContext,
                "bar_*",
                randomFrom(IndexComponentSelector.DATA, IndexComponentSelector.ALL_APPLICABLE),
                data,
                failures,
                false
            );
            assertThat(data, equalTo(Set.of("bar_bar", "bar_index")));
            assertThat(failures, empty());
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

            // data stream's corresponding backing indices are resolved with selector ::data
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                indicesAliasesAndDataStreamsContext,
                "foo_*",
                IndexComponentSelector.DATA,
                data,
                failures,
                false
            );
            assertThat(
                data,
                equalTo(
                    Set.of(
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis)
                    )
                )
            );
            assertThat(failures, empty());

            // data stream's corresponding backing indices are resolved with selector ::failures
            data = new HashSet<>();
            failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                indicesAliasesAndDataStreamsContext,
                "foo_*",
                IndexComponentSelector.FAILURES,
                data,
                failures,
                false
            );
            assertThat(data, equalTo(Set.of(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis))));
            assertThat(failures, empty());

            // data stream's corresponding backing indices are resolved with selector ::*
            data = new HashSet<>();
            failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                indicesAliasesAndDataStreamsContext,
                "foo_*",
                IndexComponentSelector.ALL_APPLICABLE,
                data,
                failures,
                false
            );
            assertThat(
                data,
                equalTo(
                    Set.of(
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis)
                    )
                )
            );
            assertThat(failures, empty());

            // include all wildcard adds the data stream's backing indices with selector ::data
            data = new HashSet<>();
            failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                indicesAliasesAndDataStreamsContext,
                "*",
                IndexComponentSelector.DATA,
                data,
                failures,
                false
            );
            assertThat(
                data,
                equalTo(
                    Set.of(
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        "bar_bar",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis)
                    )
                )
            );
            assertThat(failures, empty());

            // include all wildcard adds the data stream's backing indices with selector ::data
            data = new HashSet<>();
            failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                indicesAliasesAndDataStreamsContext,
                "*",
                IndexComponentSelector.ALL_APPLICABLE,
                data,
                failures,
                false
            );
            assertThat(
                data,
                equalTo(
                    Set.of(
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        "bar_bar",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis),
                        DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis)
                    )
                )
            );
            assertThat(failures, empty());
            // include all wildcard adds the data stream's backing indices with selector ::failures
            data = new HashSet<>();
            failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                indicesAliasesAndDataStreamsContext,
                "*",
                IndexComponentSelector.FAILURES,
                data,
                failures,
                false
            );
            assertThat(data, equalTo(Set.of(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis))));
            assertThat(failures, empty());
        }

        {
            IndexNameExpressionResolver.Context preserveDataStreamsContext = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.lenientExpand(),
                false,
                false,
                true,
                true,
                SystemIndexAccessLevel.NONE,
                NONE,
                NONE
            );

            // data stream is added next to the concrete indices with selector ::data
            Set<String> data = new HashSet<>();
            Set<String> failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                preserveDataStreamsContext,
                "foo_*",
                IndexComponentSelector.DATA,
                data,
                failures,
                false
            );
            assertThat(data, equalTo(Set.of("foo_index", "bar_index", "foo_foo", "foo_logs")));
            assertThat(failures, empty());

            // data stream is added next to the concrete indices with selector ::data and ::failures
            data = new HashSet<>();
            failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                preserveDataStreamsContext,
                "foo_*",
                IndexComponentSelector.ALL_APPLICABLE,
                data,
                failures,
                false
            );
            assertThat(data, equalTo(Set.of("foo_index", "bar_index", "foo_foo", "foo_logs")));
            assertThat(failures, equalTo(Set.of("foo_logs")));

            // data stream the only one included with selector ::failures
            data = new HashSet<>();
            failures = new HashSet<>();
            WildcardExpressionResolver.processWildcards(
                preserveDataStreamsContext,
                "foo_*",
                IndexComponentSelector.FAILURES,
                data,
                failures,
                false
            );
            assertThat(data, empty());
            assertThat(failures, equalTo(Set.of("foo_logs")));
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

        Set<String> data = new HashSet<>();
        Set<String> failures = new HashSet<>();
        WildcardExpressionResolver.processWildcards(indicesAndAliasesContext, "*", IndexComponentSelector.DATA, data, failures, false);
        assertThat(data, equalTo(Set.of("bar_bar", "foo_foo", "foo_index", "bar_index")));

        data = new HashSet<>();
        failures = new HashSet<>();
        WildcardExpressionResolver.processWildcards(onlyIndicesContext, "*", IndexComponentSelector.DATA, data, failures, false);
        assertThat(data, equalTo(Set.of("bar_bar", "foo_foo", "foo_index", "bar_index")));
        data = new HashSet<>();
        failures = new HashSet<>();
        WildcardExpressionResolver.processWildcards(indicesAndAliasesContext, "foo*", IndexComponentSelector.DATA, data, failures, false);
        assertThat(data, equalTo(Set.of("foo_foo", "foo_index", "bar_index")));
        data = new HashSet<>();
        failures = new HashSet<>();
        WildcardExpressionResolver.processWildcards(onlyIndicesContext, "foo*", IndexComponentSelector.DATA, data, failures, false);
        assertThat(data, equalTo(Set.of("foo_foo", "foo_index")));
    }

    private static IndexMetadata.Builder indexBuilder(String index, boolean hidden) {
        return IndexMetadata.builder(index)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_HIDDEN_SETTING.getKey(), hidden));
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return indexBuilder(index, false);
    }

    private static void assertWildcardResolvesToEmpty(IndexNameExpressionResolver.Context context, String wildcardExpression) {
        Set<String> data = new HashSet<>();
        Set<String> failures = new HashSet<>();
        Runnable execute = () -> WildcardExpressionResolver.processWildcards(
            context,
            wildcardExpression,
            IndexComponentSelector.DATA,
            data,
            failures,
            false
        );
        if (context.getOptions().allowNoIndices()) {
            execute.run();
            assertThat(data, empty());
            assertThat(failures, empty());
        } else {
            IndexNotFoundException infe = expectThrows(IndexNotFoundException.class, execute::run);
            assertThat(infe.getIndex().getName(), equalTo(wildcardExpression + "::data"));
        }
    }
}
