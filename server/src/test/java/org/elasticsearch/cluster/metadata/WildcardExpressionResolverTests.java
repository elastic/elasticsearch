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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFailureStore;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolverTests.resolvedExpressions;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolverTests.resolvedExpressionsSet;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolverTests.setAllowSelectors;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class WildcardExpressionResolverTests extends ESTestCase {

    private static final Predicate<String> NONE = name -> false;

    public void testConvertWildcardsJustIndicesTests() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX"))
            .put(indexBuilder("testXYY"))
            .put(indexBuilder("testYYY"))
            .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        boolean allowSelectors = randomBoolean();
        IndicesOptions indicesOptions = setAllowSelectors(
            randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()),
            allowSelectors
        );
        IndexComponentSelector selector = allowSelectors ? IndexComponentSelector.DATA : null;
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            indicesOptions,
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testXXX"))),
            equalTo(resolvedExpressionsSet(selector, "testXXX"))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testXXX", "testYYY"))
            ),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testYYY"))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testXXX", "ku*"))
            ),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "kuku"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "test*"))),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testX*"))),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXYY"))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testX*", "kuku"))
            ),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXYY", "kuku"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "*"))),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXYY", "testYYY", "kuku"))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "*", "-kuku"))
            ),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    resolvedExpressions(selector, "testX*", "-doe", "-testXXX", "-testYYY")
                )
            ),
            equalTo(resolvedExpressionsSet(selector, "testXYY"))
        );
        if (indicesOptions.ignoreUnavailable()) {
            assertThat(
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                        context,
                        resolvedExpressions(selector, "testXXX", "-testXXX")
                    )
                ),
                equalTo(resolvedExpressionsSet(selector, "testXXX", "-testXXX"))
            );
        } else {
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> IndexNameExpressionResolver.resolveExpressions(context, "testXXX", "-testXXX")
            );
            assertEquals("-testXXX", infe.getIndex().getName());
        }
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testXXX", "-testX*"))
            ),
            equalTo(resolvedExpressionsSet(selector, "testXXX"))
        );
    }

    public void testConvertWildcardsTests() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX").putAlias(AliasMetadata.builder("alias1")).putAlias(AliasMetadata.builder("alias2")))
            .put(indexBuilder("testXYY").putAlias(AliasMetadata.builder("alias2")))
            .put(indexBuilder("testYYY").putAlias(AliasMetadata.builder("alias3")))
            .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        boolean allowSelectors = randomBoolean();
        IndexComponentSelector selector = allowSelectors ? IndexComponentSelector.DATA : null;
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            setAllowSelectors(IndicesOptions.lenientExpandOpen(), allowSelectors),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testYY*", "alias*"))
            ),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "-kuku"))),
            equalTo(resolvedExpressionsSet(selector, "-kuku"))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "test*", "-testYYY"))
            ),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXYY"))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testX*", "testYYY"))
            ),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testYYY", "testX*"))
            ),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXYY", "testYYY"))
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

        boolean allowSelectors = randomBoolean();
        IndexComponentSelector selector = allowSelectors ? IndexComponentSelector.DATA : null;
        IndicesOptions openAndClosed = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().matchOpen(true).matchClosed(true))
            .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(allowSelectors))
            .build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            setAllowSelectors(openAndClosed, allowSelectors),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testX*"))),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXXY", "testXYY"))
        );
        IndicesOptions onlyClosed = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().matchOpen(false).matchClosed(true))
            .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(allowSelectors))
            .build();
        context = new IndexNameExpressionResolver.Context(state, onlyClosed, SystemIndexAccessLevel.NONE);
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testX*"))),
            equalTo(resolvedExpressionsSet(selector, "testXYY"))
        );
        IndicesOptions onlyOpen = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().matchOpen(true).matchClosed(false))
            .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(allowSelectors))
            .build();
        context = new IndexNameExpressionResolver.Context(state, onlyOpen, SystemIndexAccessLevel.NONE);
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "testX*"))),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXXY"))
        );
        IndicesOptions noExpand = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().matchNone())
            .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(allowSelectors))
            .build();
        context = new IndexNameExpressionResolver.Context(state, noExpand, SystemIndexAccessLevel.NONE);
        assertThat(IndexNameExpressionResolver.resolveExpressions(context, "testX*"), empty());
        IndicesOptions noExpandStrict = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().matchNone())
            .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(allowSelectors))
            .build();
        context = new IndexNameExpressionResolver.Context(state, noExpandStrict, SystemIndexAccessLevel.NONE);
        IndexNameExpressionResolver.Context finalContext = context;
        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> IndexNameExpressionResolver.resolveExpressions(finalContext, "testX*")
        );
        assertThat(infe.getIndex().getName(), is("testX*"));
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
        boolean allowSelectors = randomBoolean();
        IndexComponentSelector selector = allowSelectors ? IndexComponentSelector.DATA : null;
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            setAllowSelectors(IndicesOptions.lenientExpandOpen(), allowSelectors),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "test*X*"))),
            equalTo(resolvedExpressionsSet(selector, "testXXX", "testXXY", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "test*X*Y"))),
            equalTo(resolvedExpressionsSet(selector, "testXXY", "testXYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "kuku*Y*"))),
            equalTo(resolvedExpressionsSet(selector, "kukuYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "*Y*"))),
            equalTo(resolvedExpressionsSet(selector, "testXXY", "testXYY", "testYYY", "kukuYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "test*Y*X"))),
            empty()
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, resolvedExpressions(selector, "*Y*X"))),
            empty()
        );
    }

    public void testAll() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX"))
            .put(indexBuilder("testXYY"))
            .put(indexBuilder("testYYY"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndicesOptions lenientExpandOpen = IndicesOptions.lenientExpandOpen();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            lenientExpandOpen,
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, IndexComponentSelector.ALL_APPLICABLE)),
            equalTo(resolvedExpressionsSet(IndexComponentSelector.DATA, "testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.resolveExpressions(context, "_all")),
            equalTo(resolvedExpressionsSet(IndexComponentSelector.DATA, "testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.resolveExpressions(context, "*::*")),
            equalTo(resolvedExpressionsSet(IndexComponentSelector.DATA, "testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.resolveExpressions(context, "_all::data")),
            equalTo(resolvedExpressionsSet(IndexComponentSelector.DATA, "testXXX", "testXYY", "testYYY"))
        );
        assertThat(IndexNameExpressionResolver.resolveExpressions(context, "_all::failures"), empty());

        // Verify that when selectors are not allowed we do not get them back in the result.
        IndicesOptions noSelectors = setAllowSelectors(lenientExpandOpen, false);
        IndexNameExpressionResolver.Context noSelectorsContext = new IndexNameExpressionResolver.Context(
            state,
            noSelectors,
            SystemIndexAccessLevel.NONE
        );

        assertThat(
            IndexNameExpressionResolver.resolveExpressions(noSelectorsContext, "_all"),
            equalTo(resolvedExpressionsSet(null, "testXXX", "testXYY", "testYYY"))
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexNameExpressionResolver.resolveExpressions(
                noSelectorsContext,
                "_all::" + randomFrom(IndexComponentSelector.values()).getKey()
            )
        );
        assertThat(
            exception.getMessage(),
            containsString("Index component selectors are not supported in this context but found selector in expression")
        );

        IndicesOptions noExpandOptions = IndicesOptions.builder()
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().matchNone())
            .gatekeeperOptions(
                (IndicesOptions.GatekeeperOptions.builder()
                    .allowClosedIndices(randomBoolean())
                    .allowSelectors(true)
                    .allowAliasToMultipleIndices(randomBoolean())
                    .ignoreThrottled(randomBoolean()))
            )
            .build();
        IndexNameExpressionResolver.Context noExpandContext = new IndexNameExpressionResolver.Context(
            state,
            noExpandOptions,
            SystemIndexAccessLevel.NONE
        );
        assertThat(IndexNameExpressionResolver.resolveExpressions(noExpandContext, "_all"), empty());
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
                IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, IndexComponentSelector.ALL_APPLICABLE),
                empty()
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
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(
                        context,
                        randomFrom(IndexComponentSelector.ALL_APPLICABLE, IndexComponentSelector.DATA)
                    )
                ),
                equalTo(resolvedExpressionsSet(IndexComponentSelector.DATA, "index-visible-alias"))
            );
        }
    }

    public void testAllDataStreams() {

        String dataStreamName = "foo_logs";
        long epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
        IndexMetadata firstBackingIndexMetadata = createBackingIndex(dataStreamName, 1, epochMillis).build();

        IndicesOptions indicesAndAliasesOptions = IndicesOptions.builder()
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .allowEmptyExpressions(randomBoolean())
                    .matchOpen(true)
                    .matchClosed(false)
                    .resolveAliases(true)
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder().allowClosedIndices(true).allowAliasToMultipleIndices(true).ignoreThrottled(false)
            )
            .build();

        {
            // visible data streams should be returned by _all and even show backing indices are hidden
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

            String[] expressions = new String[] { DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis) };
            assertThat(
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, IndexComponentSelector.ALL_APPLICABLE)
                ),
                equalTo(resolvedExpressionsSet(IndexComponentSelector.DATA, expressions))
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

            assertThat(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, IndexComponentSelector.ALL_APPLICABLE),
                empty()
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
        boolean allowSelectors = randomBoolean();
        IndexComponentSelector selector = allowSelectors ? IndexComponentSelector.DATA : null;
        IndicesOptions onlyOpenIndicesAndAliasesDisallowNoIndicesOption = IndicesOptions.builder()
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .allowEmptyExpressions(false)
                    .matchOpen(true)
                    .matchClosed(false)
                    .includeHidden(false)
                    .resolveAliases(true)
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(randomBoolean())
                    .allowClosedIndices(randomBoolean())
                    .ignoreThrottled(randomBoolean())
                    .allowSelectors(allowSelectors)
            )
            .build();

        IndexNameExpressionResolver.Context indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            onlyOpenIndicesAndAliasesDisallowNoIndicesOption,
            randomFrom(SystemIndexAccessLevel.values())
        );
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_closed*", selector);
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_hidden_open*", selector);
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_hidden_closed*", selector);
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, ".dot_index_hidden_closed*", selector);
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "alias_closed*", selector);
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "alias_hidden*", selector);

        IndicesOptions closedAndHiddenIndicesAndAliasesDisallowNoIndicesOption = IndicesOptions.builder()
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .allowEmptyExpressions(false)
                    .matchOpen(false)
                    .matchClosed(true)
                    .includeHidden(true)
                    .resolveAliases(true)
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(randomBoolean())
                    .allowClosedIndices(randomBoolean())
                    .ignoreThrottled(randomBoolean())
                    .allowSelectors(allowSelectors)
            )
            .build();
        indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            closedAndHiddenIndicesAndAliasesDisallowNoIndicesOption,
            randomFrom(SystemIndexAccessLevel.values())
        );
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_open*", selector);
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "index_hidden_open*", selector);
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, ".dot_hidden_open*", selector);
        assertWildcardResolvesToEmpty(indicesAndAliasesContext, "alias_open*", selector);
    }

    public void testResolveAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo_foo").state(State.OPEN))
            .put(indexBuilder("bar_bar").state(State.OPEN))
            .put(indexBuilder("foo_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(indexBuilder("bar_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        boolean allowSelectors = randomBoolean();
        IndexComponentSelector selector = allowSelectors ? IndexComponentSelector.DATA : null;
        // when resolveAliases is true, WildcardExpressionResolver resolves the provided
        // expressions against the defined indices and aliases
        IndicesOptions indicesAndAliasesOptions = IndicesOptions.builder()
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .allowEmptyExpressions(randomBoolean())
                    .matchOpen(true)
                    .matchClosed(false)
                    .resolveAliases(true)
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(true)
                    .allowClosedIndices(true)
                    .ignoreThrottled(false)
                    .allowSelectors(allowSelectors)
            )
            .build();
        IndexNameExpressionResolver.Context indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            indicesAndAliasesOptions,
            SystemIndexAccessLevel.NONE
        );

        // when resolveAliases is false, WildcardExpressionResolver resolves the provided expressions only against the defined indices
        IndicesOptions skipAliasesIndicesOptions = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .allowEmptyExpressions(true)
                    .matchOpen(true)
                    .matchClosed(false)
                    .resolveAliases(false)
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(true)
                    .allowClosedIndices(true)
                    .ignoreThrottled(false)
                    .allowSelectors(allowSelectors)
            )
            .build();

        IndexNameExpressionResolver.Context skipAliasesLenientContext = new IndexNameExpressionResolver.Context(
            state,
            skipAliasesIndicesOptions,
            SystemIndexAccessLevel.NONE
        );
        // when resolveAliases is false, WildcardExpressionResolver throws an error if an empty result is not acceptable
        IndicesOptions errorOnAliasIndicesOptions = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .allowEmptyExpressions(false)
                    .matchOpen(true)
                    .matchClosed(false)
                    .resolveAliases(false)
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(true)
                    .allowClosedIndices(true)
                    .ignoreThrottled(false)
                    .allowSelectors(allowSelectors)
            )
            .build();

        IndexNameExpressionResolver.Context skipAliasesStrictContext = new IndexNameExpressionResolver.Context(
            state,
            errorOnAliasIndicesOptions,
            SystemIndexAccessLevel.NONE
        );

        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                resolvedExpressions(selector, "foo_a*")
            );
            assertThat(newHashSet(indices), equalTo(resolvedExpressionsSet(selector, "foo_index", "bar_index")));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesLenientContext,
                resolvedExpressions(selector, "foo_a*")
            );
            assertThat(indices, empty());
        }
        {
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    skipAliasesStrictContext,
                    resolvedExpressions(selector, "foo_a*")
                )
            );
            assertEquals("foo_a*", infe.getIndex().getName());
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                resolvedExpressions(selector, "foo*")
            );
            assertThat(newHashSet(indices), equalTo(resolvedExpressionsSet(selector, "foo_foo", "foo_index", "bar_index")));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesLenientContext,
                resolvedExpressions(selector, "foo*")
            );
            assertThat(newHashSet(indices), equalTo(resolvedExpressionsSet(selector, "foo_foo", "foo_index")));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesStrictContext,
                resolvedExpressions(selector, "foo*")
            );
            assertThat(newHashSet(indices), equalTo(resolvedExpressionsSet(selector, "foo_foo", "foo_index")));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                resolvedExpressions(selector, "foo_alias")
            );
            assertThat(newHashSet(indices), equalTo(resolvedExpressionsSet(selector, "foo_alias")));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesLenientContext,
                resolvedExpressions(selector, "foo_alias")
            );
            assertThat(newHashSet(indices), equalTo(resolvedExpressionsSet(selector, "foo_alias")));
        }
        {
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> IndexNameExpressionResolver.resolveExpressions(skipAliasesStrictContext, "foo_alias")
            );
            assertEquals(
                "The provided expression [foo_alias] matches an alias, specify the corresponding concrete indices instead.",
                iae.getMessage()
            );
        }
        IndicesOptions noExpandNoAliasesIndicesOptions = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(false).matchNone().resolveAliases(false))
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(true)
                    .allowClosedIndices(true)
                    .ignoreThrottled(false)
                    .allowSelectors(allowSelectors)
            )
            .build();
        IndexNameExpressionResolver.Context noExpandNoAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            noExpandNoAliasesIndicesOptions,
            SystemIndexAccessLevel.NONE
        );
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                noExpandNoAliasesContext,
                resolvedExpressions(selector, "foo_alias")
            );
            assertThat(newHashSet(indices), equalTo(resolvedExpressionsSet(selector, "foo_alias")));
        }
        IndicesOptions strictNoExpandNoAliasesIndicesOptions = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(true).matchNone().resolveAliases(false))
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(true)
                    .allowClosedIndices(true)
                    .ignoreThrottled(false)
                    .allowSelectors(allowSelectors)
            )
            .build();
        IndexNameExpressionResolver.Context strictNoExpandNoAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            strictNoExpandNoAliasesIndicesOptions,
            SystemIndexAccessLevel.NONE
        );
        {
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> IndexNameExpressionResolver.resolveExpressions(strictNoExpandNoAliasesContext, "foo_alias")
            );
            assertEquals(
                "The provided expression [foo_alias] matches an alias, specify the corresponding concrete indices instead.",
                iae.getMessage()
            );
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
            .put(secondBackingIndexMetadata, true)
            .put(firstFailureIndexMetadata, true)
            .put(secondFailureIndexMetadata, true)
            .put(
                DataStreamTestHelper.newInstance(
                    dataStreamName,
                    List.of(firstBackingIndexMetadata.getIndex(), secondBackingIndexMetadata.getIndex()),
                    List.of(firstFailureIndexMetadata.getIndex(), secondFailureIndexMetadata.getIndex())
                )
            );

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        boolean allowSelectors = randomBoolean();
        IndexComponentSelector selector = allowSelectors ? IndexComponentSelector.DATA : null;

        {
            IndicesOptions indicesAndAliasesOptions = IndicesOptions.builder()
                .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
                .wildcardOptions(
                    IndicesOptions.WildcardOptions.builder()
                        .allowEmptyExpressions(randomBoolean())
                        .matchOpen(true)
                        .matchClosed(false)
                        .resolveAliases(true)
                )
                .gatekeeperOptions(
                    IndicesOptions.GatekeeperOptions.builder()
                        .allowAliasToMultipleIndices(true)
                        .allowClosedIndices(true)
                        .ignoreThrottled(false)
                        .allowSelectors(allowSelectors)
                )
                .build();

            IndexNameExpressionResolver.Context indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
                state,
                indicesAndAliasesOptions,
                SystemIndexAccessLevel.NONE
            );

            // data streams are not included but expression matches the data stream
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                resolvedExpressions(selector, "foo_*")
            );
            assertThat(newHashSet(indices), equalTo(resolvedExpressionsSet(selector, "foo_index", "foo_foo", "bar_index")));

            // data streams are not included and expression doesn't match the data steram
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                resolvedExpressions(selector, "bar_*")
            );
            assertThat(newHashSet(indices), equalTo(resolvedExpressionsSet(selector, "bar_bar", "bar_index")));
        }

        {
            IndicesOptions indicesAndAliasesOptions = IndicesOptions.builder()
                .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
                .wildcardOptions(
                    IndicesOptions.WildcardOptions.builder()
                        .allowEmptyExpressions(randomBoolean())
                        .matchOpen(true)
                        .matchClosed(false)
                        .resolveAliases(true)
                )
                .gatekeeperOptions(
                    IndicesOptions.GatekeeperOptions.builder()
                        .allowAliasToMultipleIndices(true)
                        .allowClosedIndices(true)
                        .ignoreThrottled(false)
                        .allowSelectors(allowSelectors)
                )
                .build();
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
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesAndDataStreamsContext,
                resolvedExpressions(selector, "foo_*")
            );
            assertThat(
                newHashSet(indices),
                equalTo(
                    resolvedExpressionsSet(
                        selector,
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis)
                    )
                )
            );

            // include all wildcard adds the data stream's backing indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesAndDataStreamsContext,
                resolvedExpressions(selector, "*")
            );
            assertThat(
                newHashSet(indices),
                equalTo(
                    resolvedExpressionsSet(
                        selector,
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        "bar_bar",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis)
                    )
                )
            );
        }

        {
            IndicesOptions indicesAliasesAndExpandHiddenOptions = IndicesOptions.builder()
                .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
                .wildcardOptions(
                    IndicesOptions.WildcardOptions.builder()
                        .allowEmptyExpressions(randomBoolean())
                        .matchOpen(true)
                        .matchClosed(false)
                        .includeHidden(true)
                        .resolveAliases(true)
                )
                .gatekeeperOptions(
                    IndicesOptions.GatekeeperOptions.builder()
                        .allowAliasToMultipleIndices(true)
                        .allowClosedIndices(true)
                        .ignoreThrottled(false)
                        .allowSelectors(true)
                )
                .build();
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
            Collection<ResolvedExpression> backingIndices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                resolvedExpressions(IndexComponentSelector.DATA, "foo_*")
            );
            assertThat(
                newHashSet(backingIndices),
                equalTo(
                    resolvedExpressionsSet(
                        IndexComponentSelector.DATA,
                        "foo_index",
                        "bar_index",
                        "foo_foo",
                        DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis),
                        DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis)
                    )
                )
            );

            // data stream's corresponding backing indices are resolved
            Collection<ResolvedExpression> allIndices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                resolvedExpressions(IndexComponentSelector.ALL_APPLICABLE, "foo_*")
            );
            assertThat(
                newHashSet(allIndices),
                equalTo(
                    resolvedExpressionsSet(
                        IndexComponentSelector.DATA,
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

            // data stream's corresponding backing indices are resolved
            Collection<ResolvedExpression> failureIndices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                resolvedExpressions(IndexComponentSelector.FAILURES, "foo_*")
            );
            assertThat(
                newHashSet(failureIndices),
                equalTo(
                    resolvedExpressionsSet(
                        IndexComponentSelector.DATA,
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

        boolean allowSelectors = randomBoolean();
        IndexComponentSelector selector = allowSelectors ? IndexComponentSelector.DATA : null;

        // when resolveAliases is true, WildcardExpressionResolver resolves the provided
        // expressions against the defined indices and aliases
        IndicesOptions indicesAndAliasesOptions = IndicesOptions.builder()
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .allowEmptyExpressions(randomBoolean())
                    .matchOpen(true)
                    .matchClosed(false)
                    .resolveAliases(true)
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(true)
                    .allowClosedIndices(true)
                    .ignoreThrottled(false)
                    .allowSelectors(allowSelectors)
            )
            .build();

        IndexNameExpressionResolver.Context indicesAndAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            indicesAndAliasesOptions,
            SystemIndexAccessLevel.NONE
        );

        // when resolveAliases is false, WildcardExpressionResolver throws an error if an empty result is not acceptable
        IndicesOptions onlyIndicesOptions = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .allowEmptyExpressions(false)
                    .matchOpen(true)
                    .matchClosed(false)
                    .resolveAliases(false)
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(true)
                    .allowClosedIndices(true)
                    .ignoreThrottled(false)
                    .allowSelectors(allowSelectors)
            )
            .build();
        IndexNameExpressionResolver.Context onlyIndicesContext = new IndexNameExpressionResolver.Context(
            state,
            onlyIndicesOptions,
            SystemIndexAccessLevel.NONE
        );

        Collection<ResolvedExpression> matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
            indicesAndAliasesContext,
            resolvedExpressions(selector, "*")
        );
        assertThat(newHashSet(matches), equalTo(resolvedExpressionsSet(selector, "bar_bar", "foo_foo", "foo_index", "bar_index")));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(onlyIndicesContext, resolvedExpressions(selector, "*"));
        assertThat(newHashSet(matches), equalTo(resolvedExpressionsSet(selector, "bar_bar", "foo_foo", "foo_index", "bar_index")));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
            indicesAndAliasesContext,
            resolvedExpressions(selector, "foo*")
        );
        assertThat(newHashSet(matches), equalTo(resolvedExpressionsSet(selector, "foo_foo", "foo_index", "bar_index")));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(onlyIndicesContext, resolvedExpressions(selector, "foo*"));
        assertThat(newHashSet(matches), equalTo(resolvedExpressionsSet(selector, "foo_foo", "foo_index")));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
            indicesAndAliasesContext,
            resolvedExpressions(selector, "foo_alias")
        );
        assertThat(newHashSet(matches), equalTo(resolvedExpressionsSet(selector, "foo_alias")));
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexNameExpressionResolver.resolveExpressions(onlyIndicesContext, "foo_alias")
        );
        assertThat(
            iae.getMessage(),
            containsString("The provided expression [foo_alias] matches an alias, specify the corresponding concrete indices instead")
        );
    }

    private static IndexMetadata.Builder indexBuilder(String index, boolean hidden) {
        return IndexMetadata.builder(index)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_HIDDEN_SETTING.getKey(), hidden));
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return indexBuilder(index, false);
    }

    private static void assertWildcardResolvesToEmpty(
        IndexNameExpressionResolver.Context context,
        String wildcardExpression,
        IndexComponentSelector selector
    ) {
        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                context,
                List.of(new ResolvedExpression(wildcardExpression, selector))
            )
        );
        assertEquals(wildcardExpression, infe.getIndex().getName());
    }
}
