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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class IndexNameExpressionResolverTests extends ESTestCase {
    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);

    public void testIndexOptionsStrict() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        IndicesOptions[] indicesOptions = new IndicesOptions[]{ IndicesOptions.strictExpandOpen(), IndicesOptions.strictExpand()};
        for (IndicesOptions options : indicesOptions) {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options);
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            try {
                indexNameExpressionResolver.concreteIndexNames(context, "bar");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("bar"));
            }

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(new HashSet<>(Arrays.asList("foo", "foobar")),
                         new HashSet<>(Arrays.asList(results)));

            try {
                indexNameExpressionResolver.concreteIndexNames(context, "bar");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("bar"));
            }

            try {
                indexNameExpressionResolver.concreteIndexNames(context, "foo", "bar");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("bar"));
            }

            results = indexNameExpressionResolver.concreteIndexNames(context, "barbaz", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            try {
                indexNameExpressionResolver.concreteIndexNames(context, "barbaz", "bar");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("bar"));
            }

            results = indexNameExpressionResolver.concreteIndexNames(context, "baz*");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen());
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        results = indexNameExpressionResolver.concreteIndexNames(context, (String[])null);
        assertEquals(3, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpand());
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);

        results = indexNameExpressionResolver.concreteIndexNames(context, (String[])null);
        assertEquals(4, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen());
        results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo"));

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpand());
        results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo*");
        assertEquals(4, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo", "foofoo-closed"));
    }

    public void testIndexOptionsLenient() {
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
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = indexNameExpressionResolver.concreteIndexNames(context, "bar");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(2, results.length);
            assertEquals(new HashSet<>(Arrays.asList("foo", "foobar")),
                         new HashSet<>(Arrays.asList(results)));

            results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "bar");
            assertEquals(1, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo"));

            results = indexNameExpressionResolver.concreteIndexNames(context, "barbaz", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = indexNameExpressionResolver.concreteIndexNames(context, "barbaz", "bar");
            assertEquals(1, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo"));

            results = indexNameExpressionResolver.concreteIndexNames(context, "baz*");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        context = new IndexNameExpressionResolver.Context(state, lenientExpand);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(Arrays.toString(results), 4, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        results = indexNameExpressionResolver.concreteIndexNames(context,  "foofoo*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo"));

        context = new IndexNameExpressionResolver.Context(state, lenientExpand);
        results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo*");
        assertEquals(4, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo", "foofoo-closed"));
    }

    public void testIndexOptionsAllowUnavailableDisallowEmpty() {
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
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            try {
                indexNameExpressionResolver.concreteIndexNames(context, "bar");
                fail();
            } catch(IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("bar"));
            }

            try {
                indexNameExpressionResolver.concreteIndexNames(context, "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("baz*"));
            }

            try {
                indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("baz*"));
            }
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, expandOpen);
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        context = new IndexNameExpressionResolver.Context(state, expand);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);
    }

    public void testIndexOptionsWildcardExpansion() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("bar"))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        // Only closed
        IndicesOptions options = IndicesOptions.fromOptions(false, true, false, true);
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options);
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        // no wildcards, so wildcard expansion don't apply
        results = indexNameExpressionResolver.concreteIndexNames(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        // Only open
        options = IndicesOptions.fromOptions(false, true, true, false);
        context = new IndexNameExpressionResolver.Context(state, options);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertEquals(1, results.length);
        assertEquals("foobar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        // Open and closed
        options = IndicesOptions.fromOptions(false, true, true, true);
        context = new IndexNameExpressionResolver.Context(state, options);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar", "foo"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foobar", "foo"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "-foo*");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "-*");
        assertEquals(0, results.length);

        options = IndicesOptions.fromOptions(false, false, true, true);
        context = new IndexNameExpressionResolver.Context(state, options);
        try {
            indexNameExpressionResolver.concreteIndexNames(context, "-*");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getResourceId().toString(), equalTo("[-*]"));
        }
    }

    public void testIndexOptionsNoExpandWildcards() {
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
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "baz*");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));

            results = indexNameExpressionResolver.concreteIndexNames(context, (String[])null);
            assertEquals(0, results.length);

            results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
            assertEquals(0, results.length);
        }

        //ignore unavailable but don't allow no indices
        {
            IndicesOptions noExpandDisallowEmpty = IndicesOptions.fromOptions(true, false, false, false);
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, noExpandDisallowEmpty);
            try {
                indexNameExpressionResolver.concreteIndexNames(context, "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("baz*"));
            }

            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
        }

        //error on unavailable but allow no indices
        {
            IndicesOptions noExpandErrorUnavailable = IndicesOptions.fromOptions(false, true, false, false);
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, noExpandErrorUnavailable);
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "baz*");
            assertThat(results, emptyArray());

            try {
                indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("baz*"));
            }

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
        }

        //error on both unavailable and no indices
        {
            IndicesOptions noExpandStrict = IndicesOptions.fromOptions(false, false, false, false);
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, noExpandStrict);
            try {
                indexNameExpressionResolver.concreteIndexNames(context, "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("baz*"));
            }

            try {
                indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
                fail();
            } catch (IndexNotFoundException e) {
                assertThat(e.getIndex().getName(), equalTo("baz*"));
            }

            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
        }
    }

    public void testIndexOptionsSingleIndexNoExpandWildcards() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        //error on both unavailable and no indices + every alias needs to expand to a single index

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndexNames(context, "baz*");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getIndex().getName(), equalTo("baz*"));
        }

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getIndex().getName(), equalTo("baz*"));
        }

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            fail();
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Alias [foofoobar] has more than one indices associated with it"));
        }

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndexNames(context, "foo", "foofoobar");
            fail();
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Alias [foofoobar] has more than one indices associated with it"));
        }

        try {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
            indexNameExpressionResolver.concreteIndexNames(context, "foofoo-closed", "foofoobar");
            fail();
        } catch(IndexClosedException e) {
            assertThat(e.getMessage(), equalTo("closed"));
            assertEquals(e.getIndex().getName(), "foofoo-closed");
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed());
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "barbaz");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foofoo"));
    }

    public void testIndexOptionsEmptyCluster() {
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(MetaData.builder().build()).build();

        IndicesOptions options = IndicesOptions.strictExpandOpen();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options);
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertThat(results, emptyArray());
        try {
            indexNameExpressionResolver.concreteIndexNames(context, "foo");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getIndex().getName(), equalTo("foo"));
        }
        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertThat(results, emptyArray());
        try {
            indexNameExpressionResolver.concreteIndexNames(context, "foo*", "bar");
            fail();
        } catch (IndexNotFoundException e) {
            assertThat(e.getIndex().getName(), equalTo("bar"));
        }


        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertThat(results, emptyArray());
        results = indexNameExpressionResolver.concreteIndexNames(context, "foo");
        assertThat(results, emptyArray());
        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertThat(results, emptyArray());
        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*", "bar");
        assertThat(results, emptyArray());

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, false, true, false));
        try {
            indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        } catch (IndexNotFoundException e) {
            assertThat(e.getResourceId().toString(), equalTo("[_all]"));
        }
    }

    private IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(settings(Version.CURRENT).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }

    public void testConcreteIndicesIgnoreIndicesOneMissingIndex() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen());

        try {
            indexNameExpressionResolver.concreteIndexNames(context, "testZZZ");
            fail("Expected IndexNotFoundException");
        } catch(IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testConcreteIndicesIgnoreIndicesOneMissingIndexOtherFound() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());

        assertThat(newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testXXX", "testZZZ")), equalTo(newHashSet("testXXX")));
    }

    public void testConcreteIndicesIgnoreIndicesAllMissing() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen());

        try {
            indexNameExpressionResolver.concreteIndexNames(context, "testMo", "testMahdy");
            fail("Expected IndexNotFoundException");
        } catch(IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testConcreteIndicesIgnoreIndicesEmptyRequest() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndexNames(context, new String[]{})), equalTo(newHashSet("kuku", "testXXX")));
    }

    public void testConcreteIndicesWildcardExpansion() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX").state(State.OPEN))
                .put(indexBuilder("testXXY").state(State.OPEN))
                .put(indexBuilder("testXYY").state(State.CLOSE))
                .put(indexBuilder("testYYY").state(State.OPEN))
                .put(indexBuilder("testYYX").state(State.OPEN));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, false, false));
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testX*")), equalTo(new HashSet<String>()));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, true, false));
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testX*")), equalTo(newHashSet("testXXX", "testXXY")));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, false, true));
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testX*")), equalTo(newHashSet("testXYY")));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, true, true));
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testX*")), equalTo(newHashSet("testXXX", "testXXY", "testXYY")));
    }

    /**
     * test resolving _all pattern (null, empty array or "_all") for random IndicesOptions
     */
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
                String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(context, allIndices);
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
                String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(context, allIndices);
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
            indexNameExpressionResolver.concreteIndexNames(context, allIndices);
            fail("wildcard expansion on should trigger IndexMissingException");
        } catch (IndexNotFoundException e) {
            // expected
        }
    }

    /**
     * test resolving wildcard pattern that matches no index of alias for random IndicesOptions
     */
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
                String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(context, "Foo*");
                assertThat(concreteIndices, notNullValue());
                assertThat(concreteIndices.length, equalTo(0));
            } else {
                try {
                    indexNameExpressionResolver.concreteIndexNames(context, "Foo*");
                    fail("expecting exception when result empty and allowNoIndicec=false");
                } catch (IndexNotFoundException e) {
                    // expected exception
                }
            }
        }
    }

    public void testIsAllIndicesNull() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(null), equalTo(true));
    }

    public void testIsAllIndicesEmpty() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Collections.<String>emptyList()), equalTo(true));
    }

    public void testIsAllIndicesExplicitAll() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Arrays.asList("_all")), equalTo(true));
    }

    public void testIsAllIndicesExplicitAllPlusOther() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Arrays.asList("_all", "other")), equalTo(false));
    }

    public void testIsAllIndicesNormalIndexes() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Arrays.asList("index1", "index2", "index3")), equalTo(false));
    }

    public void testIsAllIndicesWildcard() throws Exception {
        assertThat(IndexNameExpressionResolver.isAllIndices(Arrays.asList("*")), equalTo(false));
    }

    public void testIsExplicitAllIndicesNull() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(null), equalTo(false));
    }

    public void testIsExplicitAllIndicesEmpty() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Collections.<String>emptyList()), equalTo(false));
    }

    public void testIsExplicitAllIndicesExplicitAll() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Arrays.asList("_all")), equalTo(true));
    }

    public void testIsExplicitAllIndicesExplicitAllPlusOther() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Arrays.asList("_all", "other")), equalTo(false));
    }

    public void testIsExplicitAllIndicesNormalIndexes() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Arrays.asList("index1", "index2", "index3")), equalTo(false));
    }

    public void testIsExplicitAllIndicesWildcard() throws Exception {
        assertThat(IndexNameExpressionResolver.isExplicitAllPattern(Arrays.asList("*")), equalTo(false));
    }

    public void testIsPatternMatchingAllIndicesExplicitList() throws Exception {
        //even though it does identify all indices, it's not a pattern but just an explicit list of them
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, concreteIndices, concreteIndices), equalTo(false));
    }

    public void testIsPatternMatchingAllIndicesOnlyWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(true));
    }

    public void testIsPatternMatchingAllIndicesMatchingTrailingWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"index*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(true));
    }

    public void testIsPatternMatchingAllIndicesNonMatchingTrailingWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"index*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        String[] allConcreteIndices = new String[]{"index1", "index2", "index3", "a", "b"};
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(false));
    }

    public void testIsPatternMatchingAllIndicesMatchingSingleExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"-index1", "+index1"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(true));
    }

    public void testIsPatternMatchingAllIndicesNonMatchingSingleExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"-index1"};
        String[] concreteIndices = new String[]{"index2", "index3"};
        String[] allConcreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(false));
    }

    public void testIsPatternMatchingAllIndicesMatchingTrailingWildcardAndExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"index*", "-index1", "+index1"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(true));
    }

    public void testIsPatternMatchingAllIndicesNonMatchingTrailingWildcardAndExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"index*", "-index1"};
        String[] concreteIndices = new String[]{"index2", "index3"};
        String[] allConcreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metaData, indicesOrAliases, concreteIndices), equalTo(false));
    }

    public void testIndexOptionsFailClosedIndicesAndAliases() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo1-closed").state(IndexMetaData.State.CLOSE).putAlias(AliasMetaData.builder("foobar1-closed")).putAlias(AliasMetaData.builder("foobar2-closed")))
                .put(indexBuilder("foo2-closed").state(IndexMetaData.State.CLOSE).putAlias(AliasMetaData.builder("foobar2-closed")))
                .put(indexBuilder("foo3").putAlias(AliasMetaData.builder("foobar2-closed")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpenAndForbidClosed());
        try {
            indexNameExpressionResolver.concreteIndexNames(context, "foo1-closed");
            fail("foo1-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        try {
            indexNameExpressionResolver.concreteIndexNames(context, "foobar1-closed");
            fail("foo1-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, context.getOptions().allowNoIndices(), context.getOptions().expandWildcardsOpen(), context.getOptions().expandWildcardsClosed(), context.getOptions()));
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo1-closed");
        assertThat(results, emptyArray());

        results = indexNameExpressionResolver.concreteIndexNames(context, "foobar1-closed");
        assertThat(results, emptyArray());

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        results = indexNameExpressionResolver.concreteIndexNames(context, "foo1-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo1-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "foobar1-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo1-closed"));

        // testing an alias pointing to three indices:
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpenAndForbidClosed());
        try {
            indexNameExpressionResolver.concreteIndexNames(context, "foobar2-closed");
            fail("foo2-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, context.getOptions().allowNoIndices(), context.getOptions().expandWildcardsOpen(), context.getOptions().expandWildcardsClosed(), context.getOptions()));
        results = indexNameExpressionResolver.concreteIndexNames(context, "foobar2-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo3"));

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        results = indexNameExpressionResolver.concreteIndexNames(context, "foobar2-closed");
        assertThat(results, arrayWithSize(3));
        assertThat(results, arrayContainingInAnyOrder("foo1-closed", "foo2-closed", "foo3"));
    }

    public void testDedupConcreteIndices() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("index1").putAlias(AliasMetaData.builder("alias1")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndicesOptions[] indicesOptions = new IndicesOptions[]{ IndicesOptions.strictExpandOpen(), IndicesOptions.strictExpand(),
                IndicesOptions.lenientExpandOpen(), IndicesOptions.strictExpandOpenAndForbidClosed()};
        for (IndicesOptions options : indicesOptions) {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options);
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "index1", "index1", "alias1");
            assertThat(results, equalTo(new String[]{"index1"}));
        }
    }

    private MetaData metaDataBuilder(String... indices) {
        MetaData.Builder mdBuilder = MetaData.builder();
        for (String concreteIndex : indices) {
            mdBuilder.put(indexBuilder(concreteIndex));
        }
        return mdBuilder.build();
    }

    public void testFilterClosedIndicesOnAliases() {
        MetaData.Builder mdBuilder = MetaData.builder()
            .put(indexBuilder("test-0").state(State.OPEN).putAlias(AliasMetaData.builder("alias-0")))
            .put(indexBuilder("test-1").state(IndexMetaData.State.CLOSE).putAlias(AliasMetaData.builder("alias-1")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        String[] strings = indexNameExpressionResolver.concreteIndexNames(context, "alias-*");
        assertArrayEquals(new String[] {"test-0"}, strings);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen());
        strings = indexNameExpressionResolver.concreteIndexNames(context, "alias-*");

        assertArrayEquals(new String[] {"test-0"}, strings);
    }

    public void testFilteringAliases() {
        MetaData.Builder mdBuilder = MetaData.builder()
            .put(indexBuilder("test-0").state(State.OPEN).putAlias(AliasMetaData.builder("alias-0").filter("{ \"term\": \"foo\"}")))
            .put(indexBuilder("test-1").state(State.OPEN).putAlias(AliasMetaData.builder("alias-1")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();

        String[] strings = indexNameExpressionResolver.filteringAliases(state, "test-0", "alias-*");
        assertArrayEquals(new String[] {"alias-0"}, strings);

        // concrete index supersedes filtering alias
        strings = indexNameExpressionResolver.filteringAliases(state, "test-0", "test-0,alias-*");
        assertNull(strings);

        strings = indexNameExpressionResolver.filteringAliases(state, "test-0", "test-*,alias-*");
        assertNull(strings);
    }
}
