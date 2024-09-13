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
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFailureStore;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
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

        IndicesOptions indicesOptions = randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen());
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            indicesOptions,
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("testXXX", "data")))
            ),
            equalTo(newHashSet(new ResolvedExpression("testXXX", "data")))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("testYYY", "data"))
                )
            ),
            equalTo(newHashSet(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("testYYY", "data")))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("ku*", "data"))
                )
            ),
            equalTo(newHashSet(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("kuku", "data")))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("test*", "data")))
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data")
                )
            )
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("testX*", "data")))
            ),
            equalTo(newHashSet(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("testXYY", "data")))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("testX*", "data"), new ResolvedExpression("kuku", "data"))
                )
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("kuku", "data")
                )
            )
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("*", "data")))
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data"),
                    new ResolvedExpression("kuku", "data")
                )
            )
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("*", "data"), new ResolvedExpression("-kuku", "data"))
                )
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data")
                )
            )
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(
                        new ResolvedExpression("testX*", "data"),
                        new ResolvedExpression("-doe", "data"),
                        new ResolvedExpression("-testXXX", "data"),
                        new ResolvedExpression("-testYYY", "data")
                    )
                )
            ),
            equalTo(newHashSet(new ResolvedExpression("testXYY", "data")))
        );
        if (indicesOptions == IndicesOptions.lenientExpandOpen()) {
            assertThat(
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                        context,
                        List.of(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("-testXXX", "data"))
                    )
                ),
                equalTo(newHashSet(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("-testXXX", "data")))
            );
        } else if (indicesOptions == IndicesOptions.strictExpandOpen()) {
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> IndexNameExpressionResolver.resolveExpressions(context, "testXXX", "-testXXX")
            );
            assertEquals("-testXXX", infe.getIndex().getName());
        }
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("-testX*", "data"))
                )
            ),
            equalTo(newHashSet(new ResolvedExpression("testXXX", "data")))
        );
        // A wildcard that resolves to only concrete indices: Those indices are filtered out of the results because they are
        // not compatible with the $failures selector
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("test*", "data"), new ResolvedExpression("k*", "failures"))
                )
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data")
                )
            )
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
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("testYY*", "data"), new ResolvedExpression("alias*", "data"))
                )
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data")
                )
            )
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("-kuku", "data")))
            ),
            equalTo(newHashSet(new ResolvedExpression("-kuku", "data")))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("test*", "data"), new ResolvedExpression("-testYYY", "data"))
                )
            ),
            equalTo(newHashSet(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("testXYY", "data")))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("testX*", "data"), new ResolvedExpression("testYYY", "data"))
                )
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data")
                )
            )
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("testYYY", "data"), new ResolvedExpression("testX*", "data"))
                )
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data")
                )
            )
        );
        // The alias contains only indices and those do not have a failure component to them,
        // so they are not returned from the wildcard resolver.
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    context,
                    List.of(new ResolvedExpression("a*1", "data"), new ResolvedExpression("a*2", "failures"))
                )
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data")
                )
            )
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
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("testX*", "data")))
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXXY", "data"),
                    new ResolvedExpression("testXYY", "data")
                )
            )
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, false, true),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("testX*", "data")))
            ),
            equalTo(newHashSet(new ResolvedExpression("testXYY", "data")))
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, true, false),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("testX*", "data")))
            ),
            equalTo(newHashSet(new ResolvedExpression("testXXX", "data"), new ResolvedExpression("testXXY", "data")))
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, false, false),
            SystemIndexAccessLevel.NONE
        );
        assertThat(IndexNameExpressionResolver.resolveExpressions(context, "testX*").size(), equalTo(0));
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(false, true, false, false),
            SystemIndexAccessLevel.NONE
        );
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

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("test*X*", "data")))
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXXY", "data"),
                    new ResolvedExpression("testXYY", "data")
                )
            )
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("test*X*Y", "data")))
            ),
            equalTo(newHashSet(new ResolvedExpression("testXXY", "data"), new ResolvedExpression("testXYY", "data")))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("kuku*Y*", "data")))
            ),
            equalTo(newHashSet(new ResolvedExpression("kukuYYY", "data")))
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("*Y*", "data")))
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXY", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data"),
                    new ResolvedExpression("kukuYYY", "data")
                )
            )
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("test*Y*X", "data")))
            ).size(),
            equalTo(0)
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolve(context, List.of(new ResolvedExpression("*Y*X", "data")))
            ).size(),
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
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, EnumSet.of(IndicesOptions.Selectors.DATA))
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data")
                )
            )
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, EnumSet.of(IndicesOptions.Selectors.FAILURES))
            ),
            equalTo(newHashSet())
        );
        assertThat(
            newHashSet(
                IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(
                    context,
                    EnumSet.of(IndicesOptions.Selectors.DATA, IndicesOptions.Selectors.FAILURES)
                )
            ),
            equalTo(
                newHashSet(
                    new ResolvedExpression("testXXX", "data"),
                    new ResolvedExpression("testXYY", "data"),
                    new ResolvedExpression("testYYY", "data")
                )
            )
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.resolveExpressions(context, "_all")),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.resolveExpressions(context, "_all$data")),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY"))
        );
        assertThat(
            newHashSet(IndexNameExpressionResolver.resolveExpressions(context, "_all$failures")),
            equalTo(newHashSet())
        );
        IndicesOptions noExpandOptions = IndicesOptions.fromOptions(
            randomBoolean(),
            true,
            false,
            false,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
        IndexNameExpressionResolver.Context noExpandContext = new IndexNameExpressionResolver.Context(
            state,
            noExpandOptions,
            SystemIndexAccessLevel.NONE
        );
        assertThat(IndexNameExpressionResolver.resolveExpressions(noExpandContext, "_all").size(), equalTo(0));
        assertThat(IndexNameExpressionResolver.resolveExpressions(noExpandContext, "_all$data").size(), equalTo(0));
        assertThat(IndexNameExpressionResolver.resolveExpressions(noExpandContext, "_all$failures").size(), equalTo(0));
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
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, EnumSet.of(IndicesOptions.Selectors.DATA))
                ),
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
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, EnumSet.of(IndicesOptions.Selectors.DATA))
                ),
                equalTo(newHashSet(new ResolvedExpression("index-visible-alias", "data")))
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
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, EnumSet.of(IndicesOptions.Selectors.DATA))
                ),
                equalTo(newHashSet(new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), "data")))
            );
            assertThat(
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(
                        context,
                        EnumSet.of(IndicesOptions.Selectors.DATA, IndicesOptions.Selectors.FAILURES)
                    )
                ),
                equalTo(
                    newHashSet(
                        new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), "data"),
                        new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), "data")
                    )
                )
            );
        }

        {
            // if data stream itself is hidden, backing indices should not be returned
            var dataStream = DataStream.builder(
                dataStreamName,
                List.of(firstBackingIndexMetadata.getIndex())
            )
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

            assertThat(
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(context, EnumSet.of(IndicesOptions.Selectors.DATA))
                ),
                equalTo(newHashSet())
            );
            assertThat(
                newHashSet(
                    IndexNameExpressionResolver.WildcardExpressionResolver.resolveAll(
                        context,
                        EnumSet.of(IndicesOptions.Selectors.DATA, IndicesOptions.Selectors.FAILURES)
                    )
                ),
                equalTo(newHashSet())
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
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                List.of(new ResolvedExpression("foo_a*", "data"))
            );
            assertThat(
                indices,
                containsInAnyOrder(new ResolvedExpression("foo_index", "data"), new ResolvedExpression("bar_index", "data"))
            );
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesLenientContext,
                List.of(new ResolvedExpression("foo_a*", "data"))
            );
            assertEquals(0, indices.size());
        }
        {
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                    skipAliasesStrictContext,
                    List.of(new ResolvedExpression("foo_a*", "data"))
                )
            );
            assertEquals("foo_a*", infe.getIndex().getName());
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                List.of(new ResolvedExpression("foo*", "data"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_foo", "data"),
                    new ResolvedExpression("foo_index", "data"),
                    new ResolvedExpression("bar_index", "data")
                )
            );
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesLenientContext,
                List.of(new ResolvedExpression("foo*", "data"))
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("foo_foo", "data"), new ResolvedExpression("foo_index", "data")));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesStrictContext,
                List.of(new ResolvedExpression("foo*", "data"))
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("foo_foo", "data"), new ResolvedExpression("foo_index", "data")));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                List.of(new ResolvedExpression("foo_alias", "data"))
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("foo_alias", "data")));
        }
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                skipAliasesLenientContext,
                List.of(new ResolvedExpression("foo_alias", "data"))
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("foo_alias", "data")));
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
        IndicesOptions noExpandNoAliasesIndicesOptions = IndicesOptions.fromOptions(true, false, false, false, true, false, true, false);
        IndexNameExpressionResolver.Context noExpandNoAliasesContext = new IndexNameExpressionResolver.Context(
            state,
            noExpandNoAliasesIndicesOptions,
            SystemIndexAccessLevel.NONE
        );
        {
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                noExpandNoAliasesContext,
                List.of(new ResolvedExpression("foo_alias", "data"))
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("foo_alias", "data")));
        }
        IndicesOptions strictNoExpandNoAliasesIndicesOptions = IndicesOptions.fromOptions(
            false,
            true,
            false,
            false,
            true,
            false,
            true,
            false
        );
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
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                List.of(new ResolvedExpression("foo_*", "data"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", "data"),
                    new ResolvedExpression("foo_foo", "data"),
                    new ResolvedExpression("bar_index", "data")
                )
            );

            // data streams are not included and expression doesn't match the data steram
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAndAliasesContext,
                List.of(new ResolvedExpression("bar_*", "data"))
            );
            assertThat(indices, containsInAnyOrder(new ResolvedExpression("bar_bar", "data"), new ResolvedExpression("bar_index", "data")));
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
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesAndDataStreamsContext,
                List.of(new ResolvedExpression("foo_*", "data"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", "data"),
                    new ResolvedExpression("bar_index", "data"),
                    new ResolvedExpression("foo_foo", "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), "data")
                )
            );

            // data stream's corresponding failure indices are resolved
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesAndDataStreamsContext,
                List.of(new ResolvedExpression("foo_*", "failures"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), "data")
                )
            );

            // data stream's corresponding backing and failure indices are resolved
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesAndDataStreamsContext,
                List.of(new ResolvedExpression("foo_*", "data"), new ResolvedExpression("foo_*", "failures"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", "data"),
                    new ResolvedExpression("bar_index", "data"),
                    new ResolvedExpression("foo_foo", "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), "data")
                )
            );

            // include all wildcard adds the data stream's backing indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesAndDataStreamsContext,
                List.of(new ResolvedExpression("*", "data"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", "data"),
                    new ResolvedExpression("bar_index", "data"),
                    new ResolvedExpression("foo_foo", "data"),
                    new ResolvedExpression("bar_bar", "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), "data")
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
            Collection<ResolvedExpression> indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                List.of(new ResolvedExpression("foo_*", "data"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", "data"),
                    new ResolvedExpression("bar_index", "data"),
                    new ResolvedExpression("foo_foo", "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), "data")
                )
            );

            // only data stream's corresponding failure indices are resolved
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                List.of(new ResolvedExpression("foo_*", "failures"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), "data")
                )
            );

            // Resolve both backing and failure indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                List.of(new ResolvedExpression("foo_*", "data"), new ResolvedExpression("foo_*", "failures"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", "data"),
                    new ResolvedExpression("bar_index", "data"),
                    new ResolvedExpression("foo_foo", "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), "data")
                )
            );

            // include all wildcard adds the data stream's backing indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                List.of(new ResolvedExpression("*", "data"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", "data"),
                    new ResolvedExpression("bar_index", "data"),
                    new ResolvedExpression("foo_foo", "data"),
                    new ResolvedExpression("bar_bar", "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), "data")
                )
            );

            // include all wildcard adds the data stream's failure indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                List.of(new ResolvedExpression("*", "failures"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), "data")
                )
            );

            // include all wildcard adds the data stream's backing and failure indices
            indices = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                indicesAliasesDataStreamsAndHiddenIndices,
                List.of(new ResolvedExpression("*", "data"), new ResolvedExpression("*", "failures"))
            );
            assertThat(
                indices,
                containsInAnyOrder(
                    new ResolvedExpression("foo_index", "data"),
                    new ResolvedExpression("bar_index", "data"),
                    new ResolvedExpression("foo_foo", "data"),
                    new ResolvedExpression("bar_bar", "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultBackingIndexName("foo_logs", 2, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 1, epochMillis), "data"),
                    new ResolvedExpression(DataStream.getDefaultFailureStoreName("foo_logs", 2, epochMillis), "data")
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

        Collection<ResolvedExpression> matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
            indicesAndAliasesContext,
            List.of(new ResolvedExpression("*", "data"))
        );
        assertThat(
            matches,
            containsInAnyOrder(
                new ResolvedExpression("bar_bar", "data"),
                new ResolvedExpression("foo_foo", "data"),
                new ResolvedExpression("foo_index", "data"),
                new ResolvedExpression("bar_index", "data")
            )
        );
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
            onlyIndicesContext,
            List.of(new ResolvedExpression("*", "data"))
        );
        assertThat(
            matches,
            containsInAnyOrder(
                new ResolvedExpression("bar_bar", "data"),
                new ResolvedExpression("foo_foo", "data"),
                new ResolvedExpression("foo_index", "data"),
                new ResolvedExpression("bar_index", "data")
            )
        );
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
            indicesAndAliasesContext,
            List.of(new ResolvedExpression("foo*", "data"))
        );
        assertThat(
            matches,
            containsInAnyOrder(
                new ResolvedExpression("foo_foo", "data"),
                new ResolvedExpression("foo_index", "data"),
                new ResolvedExpression("bar_index", "data")
            )
        );
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
            onlyIndicesContext,
            List.of(new ResolvedExpression("foo*", "data"))
        );
        assertThat(matches, containsInAnyOrder(new ResolvedExpression("foo_foo", "data"), new ResolvedExpression("foo_index", "data")));
        matches = IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
            indicesAndAliasesContext,
            List.of(new ResolvedExpression("foo_alias", "data"))
        );
        assertThat(matches, containsInAnyOrder(new ResolvedExpression("foo_alias", "data")));
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

    private static void assertWildcardResolvesToEmpty(IndexNameExpressionResolver.Context context, String wildcardExpression) {
        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> IndexNameExpressionResolver.WildcardExpressionResolver.resolve(
                context,
                List.of(new ResolvedExpression(wildcardExpression, "data"))
            )
        );
        assertEquals(wildcardExpression, infe.getIndex().getName());
    }
}
