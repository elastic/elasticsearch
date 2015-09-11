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
import org.elasticsearch.cluster.metadata.IndexMetaData.State;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class IndexNameExpressionResolverTests extends ESTestCase {

    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);

    @Test
    public void testIndexOptions_strict() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        IndicesOptions[] indicesOptions = new IndicesOptions[]{ IndicesOptions.strictExpandOpen(), IndicesOptions.strictExpand()};
        for (IndicesOptions options : indicesOptions) {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options);
            String[] results = indexNameExpressionResolver.concreteIndices(context, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            try {
                indexNameExpressionResolver.concreteIndices(context, "bar");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("bar"));
            }

            results = indexNameExpressionResolver.concreteIndices(context, "foofoo", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = indexNameExpressionResolver.concreteIndices(context, "foofoobar");
            assertEquals(new HashSet<>(Arrays.asList("foo", "foobar")), 
                         new HashSet<>(Arrays.asList(results)));

            try {
                indexNameExpressionResolver.concreteIndices(context, "bar");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("bar"));
            }

            try {
                indexNameExpressionResolver.concreteIndices(context, "foo", "bar");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("bar"));
            }

            results = indexNameExpressionResolver.concreteIndices(context, "barbaz", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            try {
                indexNameExpressionResolver.concreteIndices(context, "barbaz", "bar");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("bar"));
            }

            results = indexNameExpressionResolver.concreteIndices(context, "baz*");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndices(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen());
        String[] results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        results = indexNameExpressionResolver.concreteIndices(context, (String[])null);
        assertEquals(3, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpand());
        results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);

        results = indexNameExpressionResolver.concreteIndices(context, (String[])null);
        assertEquals(4, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen());
        results = indexNameExpressionResolver.concreteIndices(context, "foofoo*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo"));

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpand());
        results = indexNameExpressionResolver.concreteIndices(context, "foofoo*");
        assertEquals(4, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo", "foofoo-closed"));
    }

    @Test
    public void testIndexOptions_lenient() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        IndicesOptions lenientExpand = IndicesOptions.fromOptions(true, true, true, true);
        IndicesOptions[] indicesOptions = new IndicesOptions[]{ IndicesOptions.lenientExpandOpen(), lenientExpand};
        for (IndicesOptions options : indicesOptions) {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options);
            String[] results = indexNameExpressionResolver.concreteIndices(context, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = indexNameExpressionResolver.concreteIndices(context, "bar");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndices(context, "foofoo", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = indexNameExpressionResolver.concreteIndices(context, "foofoobar");
            assertEquals(2, results.length);
            assertEquals(new HashSet<>(Arrays.asList("foo", "foobar")), 
                         new HashSet<>(Arrays.asList(results)));

            results = indexNameExpressionResolver.concreteIndices(context, "foo", "bar");
            assertEquals(1, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo"));

            results = indexNameExpressionResolver.concreteIndices(context, "barbaz", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = indexNameExpressionResolver.concreteIndices(context, "barbaz", "bar");
            assertEquals(1, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo"));

            results = indexNameExpressionResolver.concreteIndices(context, "baz*");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndices(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        String[] results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        context = new IndexNameExpressionResolver.Context(state, lenientExpand);
        results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        results = indexNameExpressionResolver.concreteIndices(context,  "foofoo*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo"));

        context = new IndexNameExpressionResolver.Context(state, lenientExpand);
        results = indexNameExpressionResolver.concreteIndices(context, "foofoo*");
        assertEquals(4, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo", "foofoo-closed"));
    }

    @Test
    public void testIndexOptions_allowUnavailableDisallowEmpty() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo"))
                .put(indexBuilder("foobar"))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        IndicesOptions expandOpen = IndicesOptions.fromOptions(true, false, true, false);
        IndicesOptions expand = IndicesOptions.fromOptions(true, false, true, true);
        IndicesOptions[] indicesOptions = new IndicesOptions[]{expandOpen, expand};

        for (IndicesOptions options : indicesOptions) {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options);
            String[] results = indexNameExpressionResolver.concreteIndices(context, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            try {
                indexNameExpressionResolver.concreteIndices(context, "bar");
                fail();
            } catch(IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("bar"));
            }

            try {
                indexNameExpressionResolver.concreteIndices(context, "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("baz*"));
            }

            try {
                indexNameExpressionResolver.concreteIndices(context, "foo", "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("baz*"));
            }
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, expandOpen);
        String[] results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        context = new IndexNameExpressionResolver.Context(state, expand);
        results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);
    }

    @Test
    public void testIndexOptions_wildcardExpansion() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("bar"))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        // Only closed
        IndicesOptions options = IndicesOptions.fromOptions(false, true, false, true);
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options);
        String[] results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        results = indexNameExpressionResolver.concreteIndices(context, "foo*");
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        // no wildcards, so wildcard expansion don't apply
        results = indexNameExpressionResolver.concreteIndices(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        // Only open
        options = IndicesOptions.fromOptions(false, true, true, false);
        context = new IndexNameExpressionResolver.Context(state, options);
        results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar"));

        results = indexNameExpressionResolver.concreteIndices(context, "foo*");
        assertEquals(1, results.length);
        assertEquals("foobar", results[0]);

        results = indexNameExpressionResolver.concreteIndices(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        // Open and closed
        options = IndicesOptions.fromOptions(false, true, true, true);
        context = new IndexNameExpressionResolver.Context(state, options);
        results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar", "foo"));

        results = indexNameExpressionResolver.concreteIndices(context, "foo*");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foobar", "foo"));

        results = indexNameExpressionResolver.concreteIndices(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndices(context, "-foo*");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndices(context, "-*");
        assertEquals(0, results.length);

        options = IndicesOptions.fromOptions(false, false, true, true);
        context = new IndexNameExpressionResolver.Context(state, options);
        try {
            indexNameExpressionResolver.concreteIndices(context, "-*");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getResourceId().toString(), equalTo("[-*]"));
        }
    }

    @Test
    public void testIndexOptions_noExpandWildcards() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        //ignore unavailable and allow no indices
        {
            IndicesOptions noExpandLenient = IndicesOptions.fromOptions(true, true, false, false);
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, noExpandLenient);
            String[] results = indexNameExpressionResolver.concreteIndices(context, "baz*");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndices(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = indexNameExpressionResolver.concreteIndices(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));

            results = indexNameExpressionResolver.concreteIndices(context, (String[])null);
            assertEquals(0, results.length);

            results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
            assertEquals(0, results.length);
        }

        //ignore unavailable but don't allow no indices
        {
            IndicesOptions noExpandDisallowEmpty = IndicesOptions.fromOptions(true, false, false, false);
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, noExpandDisallowEmpty);
            try {
                indexNameExpressionResolver.concreteIndices(context, "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("baz*"));
            }

            String[] results = indexNameExpressionResolver.concreteIndices(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = indexNameExpressionResolver.concreteIndices(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
        }

        //error on unavailable but allow no indices
        {
            IndicesOptions noExpandErrorUnavailable = IndicesOptions.fromOptions(false, true, false, false);
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, noExpandErrorUnavailable);
            String[] results = indexNameExpressionResolver.concreteIndices(context, "baz*");
            assertThat(results, emptyArray());

            try {
                indexNameExpressionResolver.concreteIndices(context, "foo", "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("baz*"));
            }

            results = indexNameExpressionResolver.concreteIndices(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
        }

        //error on both unavailable and no indices
        {
            IndicesOptions noExpandStrict = IndicesOptions.fromOptions(false, false, false, false);
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, noExpandStrict);
            try {
                indexNameExpressionResolver.concreteIndices(context, "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("baz*"));
            }

            try {
                indexNameExpressionResolver.concreteIndices(context, "foo", "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex(), equalTo("baz*"));
            }

            String[] results = indexNameExpressionResolver.concreteIndices(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
        }
    }

    @Test
    public void testIndexOptions_singleIndexNoExpandWildcards() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        //error on both unavailable and no indices + every alias needs to expand to a single index

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndices(context, "baz*");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getIndex(), equalTo("baz*"));
        }

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndices(context, "foo", "baz*");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getIndex(), equalTo("baz*"));
        }

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndices(context, "foofoobar");
            fail();
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Alias [foofoobar] has more than one indices associated with it"));
        }

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndices(context, "foo", "foofoobar");
            fail();
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Alias [foofoobar] has more than one indices associated with it"));
        }

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndices(context, "foofoo-closed", "foofoobar");
            fail();
        } catch(IndexClosedException e) {
            assertThat(e.getMessage(), equalTo("closed"));
            assertEquals(e.getIndex(), "foofoo-closed");
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
        String[] results = indexNameExpressionResolver.concreteIndices(context, "foo", "barbaz");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foofoo"));
    }

    @Test
    public void testIndexOptions_emptyCluster() {
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(MetaData.builder().build()).build();

        IndicesOptions options = IndicesOptions.strictExpandOpen();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options);
        String[] results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertThat(results, emptyArray());
        try {
            indexNameExpressionResolver.concreteIndices(context, "foo");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getIndex(), equalTo("foo"));
        }
        results = indexNameExpressionResolver.concreteIndices(context, "foo*");
        assertThat(results, emptyArray());
        try {
            indexNameExpressionResolver.concreteIndices(context, "foo*", "bar");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getIndex(), equalTo("bar"));
        }


        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        results = indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        assertThat(results, emptyArray());
        results = indexNameExpressionResolver.concreteIndices(context, "foo");
        assertThat(results, emptyArray());
        results = indexNameExpressionResolver.concreteIndices(context, "foo*");
        assertThat(results, emptyArray());
        results = indexNameExpressionResolver.concreteIndices(context, "foo*", "bar");
        assertThat(results, emptyArray());

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, false, true, false));
        try {
            indexNameExpressionResolver.concreteIndices(context, Strings.EMPTY_ARRAY);
        } catch (IndexNotFoundException e) {
            assertThat(e.getResourceId().toString(), equalTo("[_all]"));
        }
    }

    private IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(settings(Version.CURRENT).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }

    @Test(expected = IndexNotFoundException.class)
    public void testConcreteIndicesIgnoreIndicesOneMissingIndex() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen());

        indexNameExpressionResolver.concreteIndices(context, "testZZZ");
    }

    @Test
    public void testConcreteIndicesIgnoreIndicesOneMissingIndexOtherFound() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());

        assertThat(newHashSet(indexNameExpressionResolver.concreteIndices(context, "testXXX", "testZZZ")), equalTo(newHashSet("testXXX")));
    }

    @Test(expected = IndexNotFoundException.class)
    public void testConcreteIndicesIgnoreIndicesAllMissing() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen());

        assertThat(newHashSet(indexNameExpressionResolver.concreteIndices(context, "testMo", "testMahdy")), equalTo(newHashSet("testXXX")));
    }

    @Test
    public void testConcreteIndicesIgnoreIndicesEmptyRequest() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndices(context, new String[]{})), equalTo(newHashSet("kuku", "testXXX")));
    }

    @Test
    public void testConcreteIndicesWildcardExpansion() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX").state(State.OPEN))
                .put(indexBuilder("testXXY").state(State.OPEN))
                .put(indexBuilder("testXYY").state(State.CLOSE))
                .put(indexBuilder("testYYY").state(State.OPEN))
                .put(indexBuilder("testYYX").state(State.OPEN));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, false, false));
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndices(context, "testX*")), equalTo(new HashSet<String>()));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, true, false));
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndices(context, "testX*")), equalTo(newHashSet("testXXX", "testXXY")));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, false, true));
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndices(context, "testX*")), equalTo(newHashSet("testXYY")));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, true, true));
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndices(context, "testX*")), equalTo(newHashSet("testXXX", "testXXY", "testXYY")));
    }

    /**
     * test resolving _all pattern (null, empty array or "_all") for random IndicesOptions
     */
    @Test
    public void testConcreteIndicesAllPatternRandom() {
        for (int i = 0; i < 10; i++) {
            String[] allIndices = null;
            switch (randomIntBetween(0, 2)) {
            case 0:
                break;
            case 1:
                allIndices = new String[0];
                break;
            case 2:
                allIndices = new String[] { MetaData.ALL };
                break;
            }

            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
            ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(MetaData.builder().build()).build();
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, indicesOptions);

            // with no indices, asking for all indices should return empty list or exception, depending on indices options
            if (indicesOptions.allowNoIndices()) {
                String[] concreteIndices = indexNameExpressionResolver.concreteIndices(context, allIndices);
                assertThat(concreteIndices, notNullValue());
                assertThat(concreteIndices.length, equalTo(0));
            } else {
                checkCorrectException(indexNameExpressionResolver, context, allIndices);
            }

            // with existing indices, asking for all indices should return all open/closed indices depending on options
            MetaData.Builder mdBuilder = MetaData.builder()
                    .put(indexBuilder("aaa").state(State.OPEN).putAlias(AliasMetaData.builder("aaa_alias1")))
                    .put(indexBuilder("bbb").state(State.OPEN).putAlias(AliasMetaData.builder("bbb_alias1")))
                    .put(indexBuilder("ccc").state(State.CLOSE).putAlias(AliasMetaData.builder("ccc_alias1")));
            state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
            context = new IndexNameExpressionResolver.Context(state, indicesOptions);
            if (indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed() || indicesOptions.allowNoIndices()) {
                String[] concreteIndices = indexNameExpressionResolver.concreteIndices(context, allIndices);
                assertThat(concreteIndices, notNullValue());
                int expectedNumberOfIndices = 0;
                if (indicesOptions.expandWildcardsOpen()) {
                    expectedNumberOfIndices += 2;
                }
                if (indicesOptions.expandWildcardsClosed()) {
                    expectedNumberOfIndices += 1;
                }
                assertThat(concreteIndices.length, equalTo(expectedNumberOfIndices));
            } else {
                checkCorrectException(indexNameExpressionResolver, context, allIndices);
            }
        }
    }

    /**
     * check for correct exception type depending on indicesOptions and provided index name list
     */
    private void checkCorrectException(IndexNameExpressionResolver indexNameExpressionResolver, IndexNameExpressionResolver.Context context, String[] allIndices) {
        try {
            indexNameExpressionResolver.concreteIndices(context, allIndices);
            fail("wildcard expansion on should trigger IndexMissingException");
        } catch (IndexNotFoundException e) {
            // expected
        }
    }

    /**
     * test resolving wildcard pattern that matches no index of alias for random IndicesOptions
     */
    @Test
    public void testConcreteIndicesWildcardNoMatch() {
        for (int i = 0; i < 10; i++) {
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
            MetaData.Builder mdBuilder = MetaData.builder()
                    .put(indexBuilder("aaa").state(State.OPEN).putAlias(AliasMetaData.builder("aaa_alias1")))
                    .put(indexBuilder("bbb").state(State.OPEN).putAlias(AliasMetaData.builder("bbb_alias1")))
                    .put(indexBuilder("ccc").state(State.CLOSE).putAlias(AliasMetaData.builder("ccc_alias1")));
            ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, indicesOptions);

            // asking for non existing wildcard pattern should return empty list or exception
            if (indicesOptions.allowNoIndices()) {
                String[] concreteIndices = indexNameExpressionResolver.concreteIndices(context, "Foo*");
                assertThat(concreteIndices, notNullValue());
                assertThat(concreteIndices.length, equalTo(0));
            } else {
                try {
                    indexNameExpressionResolver.concreteIndices(context, "Foo*");
                    fail("expecting exeption when result empty and allowNoIndicec=false");
                } catch (IndexNotFoundException e) {
                    // expected exception
                }
            }
        }
    }

    @Test
    public void testIsAllIndices_null() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(null), equalTo(true));
    }

    @Test
    public void testIsAllIndices_empty() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Collections.<String>emptyList()), equalTo(true));
    }

    @Test
    public void testIsAllIndices_explicitAll() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Arrays.asList("_all")), equalTo(true));
    }

    @Test
    public void testIsAllIndices_explicitAllPlusOther() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Arrays.asList("_all", "other")), equalTo(false));
    }

    @Test
    public void testIsAllIndices_normalIndexes() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Arrays.asList("index1", "index2", "index3")), equalTo(false));
    }

    @Test
    public void testIsAllIndices_wildcard() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Arrays.asList("*")), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_null() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(null), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_empty() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Collections.<String>emptyList()), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_explicitAll() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Arrays.asList("_all")), equalTo(true));
    }

    @Test
    public void testIsExplicitAllIndices_explicitAllPlusOther() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Arrays.asList("_all", "other")), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_normalIndexes() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Arrays.asList("index1", "index2", "index3")), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_wildcard() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Arrays.asList("*")), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_explicitList() throws Exception {
        //even though it does identify all indices, it's not a pattern but just an explicit list of them
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, concreteIndices, concreteIndices), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_onlyWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(true));
    }

    @Test
    public void testIsPatternMatchingAllIndices_matchingTrailingWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"index*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(true));
    }

    @Test
    public void testIsPatternMatchingAllIndices_nonMatchingTrailingWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"index*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        String[] allConcreteIndices = new String[]{"index1", "index2", "index3", "a", "b"};
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_matchingSingleExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"-index1", "+index1"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(true));
    }

    @Test
    public void testIsPatternMatchingAllIndices_nonMatchingSingleExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"-index1"};
        String[] concreteIndices = new String[]{"index2", "index3"};
        String[] allConcreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_matchingTrailingWildcardAndExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"index*", "-index1", "+index1"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(true));
    }

    @Test
    public void testIsPatternMatchingAllIndices_nonMatchingTrailingWildcardAndExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"index*", "-index1"};
        String[] concreteIndices = new String[]{"index2", "index3"};
        String[] allConcreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(false));
    }

    @Test
    public void testIndexOptions_failClosedIndicesAndAliases() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo1-closed").state(IndexMetaData.State.CLOSE).putAlias(AliasMetaData.builder("foobar1-closed")).putAlias(AliasMetaData.builder("foobar2-closed")))
                .put(indexBuilder("foo2-closed").state(IndexMetaData.State.CLOSE).putAlias(AliasMetaData.builder("foobar2-closed")))
                .put(indexBuilder("foo3").putAlias(AliasMetaData.builder("foobar2-closed")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpenAndForbidClosed());
        try {
            indexNameExpressionResolver.concreteIndices(context, "foo1-closed");
            fail("foo1-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        try {
            indexNameExpressionResolver.concreteIndices(context, "foobar1-closed");
            fail("foo1-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, context.getOptions().allowNoIndices(), context.getOptions().expandWildcardsOpen(), context.getOptions().expandWildcardsClosed(), context.getOptions()));
        String[] results = indexNameExpressionResolver.concreteIndices(context, "foo1-closed");
        assertThat(results, emptyArray());

        results = indexNameExpressionResolver.concreteIndices(context, "foobar1-closed");
        assertThat(results, emptyArray());

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        results = indexNameExpressionResolver.concreteIndices(context, "foo1-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo1-closed"));

        results = indexNameExpressionResolver.concreteIndices(context, "foobar1-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo1-closed"));

        // testing an alias pointing to three indices:
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpenAndForbidClosed());
        try {
            indexNameExpressionResolver.concreteIndices(context, "foobar2-closed");
            fail("foo2-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, context.getOptions().allowNoIndices(), context.getOptions().expandWildcardsOpen(), context.getOptions().expandWildcardsClosed(), context.getOptions()));
        results = indexNameExpressionResolver.concreteIndices(context, "foobar2-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo3"));

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        results = indexNameExpressionResolver.concreteIndices(context, "foobar2-closed");
        assertThat(results, arrayWithSize(3));
        assertThat(results, arrayContainingInAnyOrder("foo1-closed", "foo2-closed", "foo3"));
    }

    private MetaData metaDataBuilder(String... indices) {
        MetaData.Builder mdBuilder = MetaData.builder();
        for (String concreteIndex : indices) {
            mdBuilder.put(indexBuilder(concreteIndex));
        }
        return mdBuilder.build();
    }
}
