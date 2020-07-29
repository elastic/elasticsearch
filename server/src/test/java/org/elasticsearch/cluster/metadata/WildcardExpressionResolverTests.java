/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class WildcardExpressionResolverTests extends ESTestCase {
    public void testConvertWildcardsJustIndicesTests() {
        Metadata.Builder mdBuilder = Metadata.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("testXYY"))
                .put(indexBuilder("testYYY"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("testXXX"))), equalTo(newHashSet("testXXX")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testXXX", "testYYY"))), equalTo(newHashSet("testXXX", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testXXX", "ku*"))), equalTo(newHashSet("testXXX", "kuku")));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("test*"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("testX*"))), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testX*", "kuku"))),
            equalTo(newHashSet("testXXX", "testXYY", "kuku")));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("*"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY", "kuku")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("*", "-kuku"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testXXX", "testYYY"))), equalTo(newHashSet("testXXX", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testXXX", "-testXXX"))), equalTo(newHashSet("testXXX", "-testXXX")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testXXX", "testY*"))), equalTo(newHashSet("testXXX", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testXXX", "-testX*"))), equalTo(newHashSet("testXXX")));
    }

    public void testConvertWildcardsTests() {
        Metadata.Builder mdBuilder = Metadata.builder()
                .put(indexBuilder("testXXX").putAlias(AliasMetadata.builder("alias1")).putAlias(AliasMetadata.builder("alias2")))
                .put(indexBuilder("testXYY").putAlias(AliasMetadata.builder("alias2")))
                .put(indexBuilder("testYYY").putAlias(AliasMetadata.builder("alias3")))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testYY*", "alias*"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("-kuku"))), equalTo(newHashSet("-kuku")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("test*", "-testYYY"))), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testX*", "testYYY")))
            , equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testYYY", "testX*"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
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
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state,
            IndicesOptions.fromOptions(true, true, true, true));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("testX*"))),
            equalTo(newHashSet("testXXX", "testXXY", "testXYY")));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, false, true));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("testX*"))), equalTo(newHashSet("testXYY")));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, true, false));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("testX*"))), equalTo(newHashSet("testXXX", "testXXY")));
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
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("test*X*"))),
            equalTo(newHashSet("testXXX", "testXXY", "testXYY")));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("test*X*Y"))), equalTo(newHashSet("testXXY", "testXYY")));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("kuku*Y*"))), equalTo(newHashSet("kukuYYY")));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("*Y*"))),
            equalTo(newHashSet("testXXY", "testXYY", "testYYY", "kukuYYY")));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("test*Y*X"))).size(), equalTo(0));
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("*Y*X"))).size(), equalTo(0));
    }

    public void testAll() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX"))
            .put(indexBuilder("testXYY"))
            .put(indexBuilder("testYYY"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(resolver.resolve(context, Collections.singletonList("_all"))),
            equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
    }

    public void testResolveAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
                .put(indexBuilder("foo_foo").state(State.OPEN))
                .put(indexBuilder("bar_bar").state(State.OPEN))
                .put(indexBuilder("foo_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
                .put(indexBuilder("bar_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();
        // when ignoreAliases option is not set, WildcardExpressionResolver resolves the provided
        // expressions against the defined indices and aliases
        IndicesOptions indicesAndAliasesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false, true, false,
            false, false);
        IndexNameExpressionResolver.Context indicesAndAliasesContext =
            new IndexNameExpressionResolver.Context(state, indicesAndAliasesOptions);
        // ignoreAliases option is set, WildcardExpressionResolver throws error when
        IndicesOptions skipAliasesIndicesOptions = IndicesOptions.fromOptions(true, true, true, false, true, false, true, false);
        IndexNameExpressionResolver.Context skipAliasesLenientContext =
            new IndexNameExpressionResolver.Context(state, skipAliasesIndicesOptions);
        // ignoreAliases option is set, WildcardExpressionResolver resolves the provided expressions only against the defined indices
        IndicesOptions errorOnAliasIndicesOptions = IndicesOptions.fromOptions(false, false, true, false, true, false, true, false);
        IndexNameExpressionResolver.Context skipAliasesStrictContext =
            new IndexNameExpressionResolver.Context(state, errorOnAliasIndicesOptions);

        {
            List<String> indices = resolver.resolve(indicesAndAliasesContext, Collections.singletonList("foo_a*"));
            assertThat(indices, containsInAnyOrder("foo_index", "bar_index"));
        }
        {
            List<String> indices = resolver.resolve(skipAliasesLenientContext, Collections.singletonList("foo_a*"));
            assertEquals(0, indices.size());
        }
        {
            IndexNotFoundException infe = expectThrows(IndexNotFoundException.class,
                    () -> resolver.resolve(skipAliasesStrictContext, Collections.singletonList("foo_a*")));
            assertEquals("foo_a*", infe.getIndex().getName());
        }
        {
            List<String> indices = resolver.resolve(indicesAndAliasesContext, Collections.singletonList("foo*"));
            assertThat(indices, containsInAnyOrder("foo_foo", "foo_index", "bar_index"));
        }
        {
            List<String> indices = resolver.resolve(skipAliasesLenientContext, Collections.singletonList("foo*"));
            assertThat(indices, containsInAnyOrder("foo_foo", "foo_index"));
        }
        {
            List<String> indices = resolver.resolve(skipAliasesStrictContext, Collections.singletonList("foo*"));
            assertThat(indices, containsInAnyOrder("foo_foo", "foo_index"));
        }
        {
            List<String> indices = resolver.resolve(indicesAndAliasesContext, Collections.singletonList("foo_alias"));
            assertThat(indices, containsInAnyOrder("foo_alias"));
        }
        {
            List<String> indices = resolver.resolve(skipAliasesLenientContext, Collections.singletonList("foo_alias"));
            assertThat(indices, containsInAnyOrder("foo_alias"));
        }
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                    () -> resolver.resolve(skipAliasesStrictContext, Collections.singletonList("foo_alias")));
            assertEquals("The provided expression [foo_alias] matches an alias, " +
                    "specify the corresponding concrete indices instead.", iae.getMessage());
        }
    }

    public void testResolveDataStreams() {
        String dataStreamName = "foo_logs";
        IndexMetadata firstBackingIndexMetadata = createBackingIndex(dataStreamName, 1).build();
        IndexMetadata secondBackingIndexMetadata = createBackingIndex(dataStreamName, 2).build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo_foo").state(State.OPEN))
            .put(indexBuilder("bar_bar").state(State.OPEN))
            .put(indexBuilder("foo_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(indexBuilder("bar_index").state(State.OPEN).putAlias(AliasMetadata.builder("foo_alias")))
            .put(firstBackingIndexMetadata, true)
            .put(secondBackingIndexMetadata, true)
            .put(new DataStream(dataStreamName, createTimestampField("@timestamp"),
                List.of(firstBackingIndexMetadata.getIndex(), secondBackingIndexMetadata.getIndex())));

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        {
            IndicesOptions indicesAndAliasesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false, true, false,
                false, false);
            IndexNameExpressionResolver.Context indicesAndAliasesContext =
                new IndexNameExpressionResolver.Context(state, indicesAndAliasesOptions);

            // data streams are not included but expression matches the data stream
            List<String> indices = resolver.resolve(indicesAndAliasesContext, Collections.singletonList("foo_*"));
            assertThat(indices, containsInAnyOrder("foo_index", "foo_foo", "bar_index"));

            // data streams are not included and expression doesn't match the data steram
            indices = resolver.resolve(indicesAndAliasesContext, Collections.singletonList("bar_*"));
            assertThat(indices, containsInAnyOrder("bar_bar", "bar_index"));
        }

        {
            IndicesOptions indicesAndAliasesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false, true, false,
                false, false);
            IndexNameExpressionResolver.Context indicesAliasesAndDataStreamsContext = new IndexNameExpressionResolver.Context(state,
                indicesAndAliasesOptions, false, false, true);

            // data stream's corresponding backing indices are resolved
            List<String> indices = resolver.resolve(indicesAliasesAndDataStreamsContext, Collections.singletonList("foo_*"));
            assertThat(indices, containsInAnyOrder("foo_index", "bar_index", "foo_foo", ".ds-foo_logs-000001",
                ".ds-foo_logs-000002"));

            // include all wildcard adds the data stream's backing indices
            indices = resolver.resolve(indicesAliasesAndDataStreamsContext, Collections.singletonList("*"));
            assertThat(indices, containsInAnyOrder("foo_index", "bar_index", "foo_foo", "bar_bar", ".ds-foo_logs-000001",
                ".ds-foo_logs-000002"));
        }

        {
            IndicesOptions indicesAliasesAndExpandHiddenOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false,
                true, true, false, false, false);
            IndexNameExpressionResolver.Context indicesAliasesDataStreamsAndHiddenIndices = new IndexNameExpressionResolver.Context(state,
                indicesAliasesAndExpandHiddenOptions, false, false, true);

            // data stream's corresponding backing indices are resolved
            List<String> indices = resolver.resolve(indicesAliasesDataStreamsAndHiddenIndices, Collections.singletonList("foo_*"));
            assertThat(indices, containsInAnyOrder("foo_index", "bar_index", "foo_foo", ".ds-foo_logs-000001",
                ".ds-foo_logs-000002"));

            // include all wildcard adds the data stream's backing indices
            indices = resolver.resolve(indicesAliasesDataStreamsAndHiddenIndices, Collections.singletonList("*"));
            assertThat(indices, containsInAnyOrder("foo_index", "bar_index", "foo_foo", "bar_bar", ".ds-foo_logs-000001",
                ".ds-foo_logs-000002"));
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
        IndexNameExpressionResolver.Context indicesAndAliasesContext =
            new IndexNameExpressionResolver.Context(state, indicesAndAliasesOptions);

        // ignoreAliases option is set, WildcardExpressionResolver resolves the provided expressions
        // only against the defined indices
        IndicesOptions onlyIndicesOptions = IndicesOptions.fromOptions(false, false, true, false, true, false, true, false);
        IndexNameExpressionResolver.Context onlyIndicesContext = new IndexNameExpressionResolver.Context(state, onlyIndicesOptions);

        {
            Set<String> matches = IndexNameExpressionResolver.WildcardExpressionResolver.matches(indicesAndAliasesContext,
                    state.getMetadata(), "*").keySet();
            assertEquals(newHashSet("bar_bar", "foo_foo", "foo_index", "bar_index", "foo_alias"), matches);
        }
        {
            Set<String> matches = IndexNameExpressionResolver.WildcardExpressionResolver.matches(onlyIndicesContext,
                    state.getMetadata(), "*").keySet();
            assertEquals(newHashSet("bar_bar", "foo_foo", "foo_index", "bar_index"), matches);
        }
        {
            Set<String> matches = IndexNameExpressionResolver.WildcardExpressionResolver.matches(indicesAndAliasesContext,
                    state.getMetadata(), "foo*").keySet();
            assertEquals(newHashSet("foo_foo", "foo_index", "foo_alias"), matches);
        }
        {
            Set<String> matches = IndexNameExpressionResolver.WildcardExpressionResolver.matches(onlyIndicesContext,
                    state.getMetadata(), "foo*").keySet();
            assertEquals(newHashSet("foo_foo", "foo_index"), matches);
        }
        {
            Set<String> matches = IndexNameExpressionResolver.WildcardExpressionResolver.matches(indicesAndAliasesContext,
                    state.getMetadata(), "foo_alias").keySet();
            assertEquals(newHashSet("foo_alias"), matches);
        }
        {
            Set<String> matches = IndexNameExpressionResolver.WildcardExpressionResolver.matches(onlyIndicesContext,
                    state.getMetadata(), "foo_alias").keySet();
            assertEquals(newHashSet(), matches);
        }
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return IndexMetadata.builder(index).settings(settings(Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
    }
}
