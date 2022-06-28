/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.Feature;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_HIDDEN_SETTING;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.indices.SystemIndices.EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY;
import static org.elasticsearch.indices.SystemIndices.SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndexNameExpressionResolverTests extends ESTestCase {

    private IndexNameExpressionResolver indexNameExpressionResolver;
    private ThreadContext threadContext;
    private long epochMillis;

    private ThreadContext createThreadContext() {
        return new ThreadContext(Settings.EMPTY);
    }

    protected IndexNameExpressionResolver createIndexNameExpressionResolver(ThreadContext threadContext) {
        return TestIndexNameExpressionResolver.newInstance(threadContext);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = createThreadContext();
        indexNameExpressionResolver = createIndexNameExpressionResolver(threadContext);
        epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
    }

    public void testIndexOptionsStrict() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo").putAlias(AliasMetadata.builder("foofoobar")))
            .put(indexBuilder("foobar").putAlias(AliasMetadata.builder("foofoobar")))
            .put(indexBuilder("foofoo-closed").state(IndexMetadata.State.CLOSE))
            .put(indexBuilder("foofoo").putAlias(AliasMetadata.builder("barbaz")));

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndicesOptions[] indicesOptions = new IndicesOptions[] { IndicesOptions.strictExpandOpen(), IndicesOptions.strictExpand() };
        for (IndicesOptions options : indicesOptions) {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                options,
                SystemIndexAccessLevel.NONE
            );
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            {
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "bar")
                );
                assertThat(infe.getIndex().getName(), equalTo("bar"));
            }

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(new HashSet<>(Arrays.asList("foo", "foobar")), new HashSet<>(Arrays.asList(results)));

            {
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "bar")
                );
                assertThat(infe.getIndex().getName(), equalTo("bar"));
            }

            {
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "foo", "bar")
                );
                assertThat(infe.getIndex().getName(), equalTo("bar"));
            }

            results = indexNameExpressionResolver.concreteIndexNames(context, "barbaz", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            {
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "barbaz", "bar")
                );
                assertThat(infe.getIndex().getName(), equalTo("bar"));
            }

            results = indexNameExpressionResolver.concreteIndexNames(context, "baz*");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.strictExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        results = indexNameExpressionResolver.concreteIndexNames(context, (String[]) null);
        assertEquals(3, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpand(), SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);

        results = indexNameExpressionResolver.concreteIndexNames(context, (String[]) null);
        assertEquals(4, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen(), SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo"));

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpand(), SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo*");
        assertEquals(4, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo", "foofoo-closed"));
    }

    public void testIndexOptionsLenient() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo").putAlias(AliasMetadata.builder("foofoobar")))
            .put(indexBuilder("foobar").putAlias(AliasMetadata.builder("foofoobar")))
            .put(indexBuilder("foofoo-closed").state(IndexMetadata.State.CLOSE))
            .put(indexBuilder("foofoo").putAlias(AliasMetadata.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndicesOptions[] indicesOptions = new IndicesOptions[] { IndicesOptions.lenientExpandOpen(), IndicesOptions.lenientExpand() };
        for (IndicesOptions options : indicesOptions) {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                options,
                SystemIndexAccessLevel.NONE
            );
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
            assertEquals(new HashSet<>(Arrays.asList("foo", "foobar")), new HashSet<>(Arrays.asList(results)));

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

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpand(), SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(Arrays.toString(results), 4, results.length);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen(), SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo"));

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpand(), SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, "foofoo*");
        assertEquals(4, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo", "foofoo-closed"));
    }

    public void testIndexOptionsAllowUnavailableDisallowEmpty() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo"))
            .put(indexBuilder("foobar"))
            .put(indexBuilder("foofoo-closed").state(IndexMetadata.State.CLOSE))
            .put(indexBuilder("foofoo").putAlias(AliasMetadata.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndicesOptions expandOpen = IndicesOptions.fromOptions(true, false, true, false);
        IndicesOptions expand = IndicesOptions.fromOptions(true, false, true, true);
        IndicesOptions[] indicesOptions = new IndicesOptions[] { expandOpen, expand };

        for (IndicesOptions options : indicesOptions) {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                options,
                SystemIndexAccessLevel.NONE
            );
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            {
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "bar")
                );
                assertThat(infe.getIndex().getName(), equalTo("bar"));
            }
            {
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "baz*")
                );
                assertThat(infe.getIndex().getName(), equalTo("baz*"));
            }
            {
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*")
                );
                assertThat(infe.getIndex().getName(), equalTo("baz*"));
            }
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            expandOpen,
            SystemIndexAccessLevel.NONE
        );
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        context = new IndexNameExpressionResolver.Context(state, expand, SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);
    }

    public void testIndexOptionsWildcardExpansion() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo").state(IndexMetadata.State.CLOSE))
            .put(indexBuilder("bar"))
            .put(indexBuilder("foobar").putAlias(AliasMetadata.builder("barbaz")))
            .put(indexBuilder("hidden", Settings.builder().put("index.hidden", true).build()))
            .put(indexBuilder(".hidden", Settings.builder().put("index.hidden", true).build()))
            .put(indexBuilder(".hidden-closed", Settings.builder().put("index.hidden", true).build()).state(IndexMetadata.State.CLOSE))
            .put(indexBuilder("hidden-closed", Settings.builder().put("index.hidden", true).build()).state(IndexMetadata.State.CLOSE));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        // Only closed
        IndicesOptions options = IndicesOptions.fromOptions(false, true, false, true, false);
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, options, SystemIndexAccessLevel.NONE);
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

        // implicit hidden for dot indices based on wildcard starting with .
        results = indexNameExpressionResolver.concreteIndexNames(context, ".*");
        assertEquals(1, results.length);
        assertThat(results, arrayContainingInAnyOrder(".hidden-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, ".hidd*");
        assertEquals(1, results.length);
        assertThat(results, arrayContainingInAnyOrder(".hidden-closed"));

        // Only open
        options = IndicesOptions.fromOptions(false, true, true, false, false);
        context = new IndexNameExpressionResolver.Context(state, options, SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertEquals(1, results.length);
        assertEquals("foobar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        // implicit hidden for dot indices based on wildcard starting with .
        results = indexNameExpressionResolver.concreteIndexNames(context, ".*");
        assertEquals(1, results.length);
        assertThat(results, arrayContainingInAnyOrder(".hidden"));

        results = indexNameExpressionResolver.concreteIndexNames(context, ".hidd*");
        assertEquals(1, results.length);
        assertThat(results, arrayContainingInAnyOrder(".hidden"));

        // Open and closed
        options = IndicesOptions.fromOptions(false, true, true, true, false);
        context = new IndexNameExpressionResolver.Context(state, options, SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar", "foo"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foobar", "foo"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "*", "-foo*");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "*", "-foo", "-foobar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "*", "-foo", "*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar", "foo"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "-*");
        assertEquals(0, results.length);

        // implicit hidden for dot indices based on wildcard starting with .
        results = indexNameExpressionResolver.concreteIndexNames(context, ".*");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder(".hidden", ".hidden-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, ".hidd*");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder(".hidden", ".hidden-closed"));

        // open closed and hidden
        options = IndicesOptions.fromOptions(false, true, true, true, true);
        context = new IndexNameExpressionResolver.Context(state, options, SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(7, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar", "foo", "hidden", "hidden-closed", ".hidden", ".hidden-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foobar", "foo"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "*", "-foo*");
        assertEquals(5, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "hidden", "hidden-closed", ".hidden", ".hidden-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "*", "-foo", "-foobar");
        assertEquals(5, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "hidden", "hidden-closed", ".hidden", ".hidden-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "*", "-foo", "-foobar", "-hidden*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", ".hidden", ".hidden-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "hidden*");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("hidden", "hidden-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "hidden");
        assertEquals(1, results.length);
        assertThat(results, arrayContainingInAnyOrder("hidden"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "hidden-closed");
        assertEquals(1, results.length);
        assertThat(results, arrayContainingInAnyOrder("hidden-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "-*");
        assertEquals(0, results.length);

        // open and hidden
        options = IndicesOptions.fromOptions(false, true, true, false, true);
        context = new IndexNameExpressionResolver.Context(state, options, SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar", "hidden", ".hidden"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertEquals(1, results.length);
        assertEquals("foobar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "h*");
        assertEquals(1, results.length);
        assertEquals("hidden", results[0]);

        // closed and hidden
        options = IndicesOptions.fromOptions(false, true, false, true, true);
        context = new IndexNameExpressionResolver.Context(state, options, SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "hidden-closed", ".hidden-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = indexNameExpressionResolver.concreteIndexNames(context, "h*");
        assertEquals(1, results.length);
        assertEquals("hidden-closed", results[0]);

        // only hidden
        options = IndicesOptions.fromOptions(false, true, false, false, true);
        context = new IndexNameExpressionResolver.Context(state, options, SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertThat(results, emptyArray());

        results = indexNameExpressionResolver.concreteIndexNames(context, "h*");
        assertThat(results, emptyArray());

        results = indexNameExpressionResolver.concreteIndexNames(context, "hidden");
        assertThat(results, arrayContainingInAnyOrder("hidden"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "hidden-closed");
        assertThat(results, arrayContainingInAnyOrder("hidden-closed"));

        options = IndicesOptions.fromOptions(false, false, true, true, true);
        IndexNameExpressionResolver.Context context2 = new IndexNameExpressionResolver.Context(state, options, SystemIndexAccessLevel.NONE);
        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> indexNameExpressionResolver.concreteIndexNames(context2, "-*")
        );
        assertThat(infe.getResourceId().toString(), equalTo("[-*]"));
    }

    public void testIndexOptionsNoExpandWildcards() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo").putAlias(AliasMetadata.builder("foofoobar")))
            .put(indexBuilder("foobar").putAlias(AliasMetadata.builder("foofoobar")))
            .put(indexBuilder("foofoo-closed").state(IndexMetadata.State.CLOSE))
            .put(indexBuilder("foofoo").putAlias(AliasMetadata.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        // ignore unavailable and allow no indices
        {
            IndicesOptions noExpandLenient = IndicesOptions.fromOptions(true, true, false, false, randomBoolean());
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                noExpandLenient,
                SystemIndexAccessLevel.NONE
            );
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "baz*");
            assertThat(results, emptyArray());

            results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));

            results = indexNameExpressionResolver.concreteIndexNames(context, (String[]) null);
            assertEquals(0, results.length);

            results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
            assertEquals(0, results.length);
        }

        // ignore unavailable but don't allow no indices
        {
            IndicesOptions noExpandDisallowEmpty = IndicesOptions.fromOptions(true, false, false, false, randomBoolean());
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                noExpandDisallowEmpty,
                SystemIndexAccessLevel.NONE
            );

            {
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "baz*")
                );
                assertThat(infe.getIndex().getName(), equalTo("baz*"));
            }

            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));

            {
                // unavailable indices are ignored but no indices are disallowed
                expectThrows(IndexNotFoundException.class, () -> indexNameExpressionResolver.concreteIndexNames(context, "bar", "baz"));
            }
        }

        // error on unavailable but allow no indices
        {
            IndicesOptions noExpandErrorUnavailable = IndicesOptions.fromOptions(false, true, false, false, randomBoolean());
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                noExpandErrorUnavailable,
                SystemIndexAccessLevel.NONE
            );
            {
                String[] results = indexNameExpressionResolver.concreteIndexNames(context, "baz*");
                assertThat(results, emptyArray());
            }
            {
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*")
                );
                assertThat(infe.getIndex().getName(), equalTo("baz*"));
            }
            {
                // unavailable indices are not ignored, hence the error on the first unavailable indices encountered
                IndexNotFoundException infe = expectThrows(
                    IndexNotFoundException.class,
                    () -> indexNameExpressionResolver.concreteIndexNames(context, "bar", "baz")
                );
                assertThat(infe.getIndex().getName(), equalTo("bar"));
            }
            {
                String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
                assertEquals(2, results.length);
                assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
            }
        }

        // error on both unavailable and no indices
        {
            IndicesOptions noExpandStrict = IndicesOptions.fromOptions(false, false, false, false, randomBoolean());
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                noExpandStrict,
                SystemIndexAccessLevel.NONE
            );
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(context, "baz*")
            );
            assertThat(infe.getIndex().getName(), equalTo("baz*"));

            IndexNotFoundException infe2 = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*")
            );
            assertThat(infe2.getIndex().getName(), equalTo("baz*"));

            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
        }
    }

    public void testIndexOptionsSingleIndexNoExpandWildcards() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo").putAlias(AliasMetadata.builder("foofoobar")))
            .put(indexBuilder("foobar").putAlias(AliasMetadata.builder("foofoobar")))
            .put(indexBuilder("foofoo-closed").state(IndexMetadata.State.CLOSE))
            .put(indexBuilder("foofoo").putAlias(AliasMetadata.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        // error on both unavailable and no indices + every alias needs to expand to a single index

        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.strictSingleIndexNoExpandForbidClosed(),
                SystemIndexAccessLevel.NONE
            );
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(context, "baz*")
            );
            assertThat(infe.getIndex().getName(), equalTo("baz*"));
        }

        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.strictSingleIndexNoExpandForbidClosed(),
                SystemIndexAccessLevel.NONE
            );
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(context, "foo", "baz*")
            );
            assertThat(infe.getIndex().getName(), equalTo("baz*"));
        }

        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.strictSingleIndexNoExpandForbidClosed(),
                SystemIndexAccessLevel.NONE
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(context, "foofoobar")
            );
            assertThat(e.getMessage(), containsString("alias [foofoobar] has more than one index associated with it"));
        }

        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.strictSingleIndexNoExpandForbidClosed(),
                SystemIndexAccessLevel.NONE
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(context, "foo", "foofoobar")
            );
            assertThat(e.getMessage(), containsString("alias [foofoobar] has more than one index associated with it"));
        }

        {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                IndicesOptions.strictSingleIndexNoExpandForbidClosed(),
                SystemIndexAccessLevel.NONE
            );
            IndexClosedException ince = expectThrows(
                IndexClosedException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(context, "foofoo-closed", "foofoobar")
            );
            assertThat(ince.getMessage(), equalTo("closed"));
            assertEquals(ince.getIndex().getName(), "foofoo-closed");
        }

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.strictSingleIndexNoExpandForbidClosed(),
            SystemIndexAccessLevel.NONE
        );
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo", "barbaz");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foofoo"));
    }

    public void testIndexOptionsEmptyCluster() {
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(Metadata.builder().build()).build();

        IndicesOptions options = IndicesOptions.strictExpandOpen();
        final IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            options,
            SystemIndexAccessLevel.NONE
        );
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, Strings.EMPTY_ARRAY);
        assertThat(results, emptyArray());

        {
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(context, "foo")
            );
            assertThat(infe.getIndex().getName(), equalTo("foo"));
        }

        results = indexNameExpressionResolver.concreteIndexNames(context, "foo*");
        assertThat(results, emptyArray());

        {
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(context, "foo*", "bar")
            );
            assertThat(infe.getIndex().getName(), equalTo("bar"));
        }

        final IndexNameExpressionResolver.Context context2 = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        results = indexNameExpressionResolver.concreteIndexNames(context2, Strings.EMPTY_ARRAY);
        assertThat(results, emptyArray());
        results = indexNameExpressionResolver.concreteIndexNames(context2, "foo");
        assertThat(results, emptyArray());
        results = indexNameExpressionResolver.concreteIndexNames(context2, "foo*");
        assertThat(results, emptyArray());
        results = indexNameExpressionResolver.concreteIndexNames(context2, "foo*", "bar");
        assertThat(results, emptyArray());

        final IndexNameExpressionResolver.Context context3 = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, false, true, false),
            SystemIndexAccessLevel.NONE
        );
        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> indexNameExpressionResolver.concreteIndexNames(context3, Strings.EMPTY_ARRAY)
        );
        assertThat(infe.getResourceId().toString(), equalTo("[_all]"));
    }

    public static IndexMetadata.Builder indexBuilder(String index) {
        return indexBuilder(index, Settings.EMPTY);
    }

    private static IndexMetadata.Builder indexBuilder(String index, Settings additionalSettings) {
        return IndexMetadata.builder(index).settings(settings(additionalSettings));
    }

    private static Settings.Builder settings(Settings additionalSettings) {
        return settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(additionalSettings);
    }

    public void testConcreteIndicesIgnoreIndicesOneMissingIndex() {
        Metadata.Builder mdBuilder = Metadata.builder().put(indexBuilder("testXXX")).put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.strictExpandOpen(),
            SystemIndexAccessLevel.NONE
        );

        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> indexNameExpressionResolver.concreteIndexNames(context, "testZZZ")
        );
        assertThat(infe.getMessage(), is("no such index [testZZZ]"));
    }

    public void testConcreteIndicesIgnoreIndicesOneMissingIndexOtherFound() {
        Metadata.Builder mdBuilder = Metadata.builder().put(indexBuilder("testXXX")).put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testXXX", "testZZZ")),
            equalTo(newHashSet("testXXX"))
        );
    }

    public void testConcreteIndicesIgnoreIndicesAllMissing() {
        Metadata.Builder mdBuilder = Metadata.builder().put(indexBuilder("testXXX")).put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.strictExpandOpen(),
            SystemIndexAccessLevel.NONE
        );

        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> indexNameExpressionResolver.concreteIndexNames(context, "testMo", "testMahdy")
        );
        assertThat(infe.getMessage(), is("no such index [testMo]"));
    }

    public void testConcreteIndicesIgnoreIndicesEmptyRequest() {
        Metadata.Builder mdBuilder = Metadata.builder().put(indexBuilder("testXXX")).put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, new String[] {})),
            equalTo(newHashSet("kuku", "testXXX"))
        );
    }

    public void testConcreteIndicesNoIndicesErrorMessage() {
        Metadata.Builder mdBuilder = Metadata.builder();
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(false, false, true, true),
            SystemIndexAccessLevel.NONE
        );
        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> indexNameExpressionResolver.concreteIndices(context, new String[] {})
        );
        assertThat(infe.getMessage(), is("no such index [null] and no indices exist"));
    }

    public void testConcreteIndicesNoIndicesErrorMessageNoExpand() {
        Metadata.Builder mdBuilder = Metadata.builder();
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(false, false, false, false),
            SystemIndexAccessLevel.NONE
        );
        IndexNotFoundException infe = expectThrows(
            IndexNotFoundException.class,
            () -> indexNameExpressionResolver.concreteIndices(context, new String[] {})
        );
        assertThat(infe.getMessage(), is("no such index [_all] and no indices exist"));
    }

    public void testConcreteIndicesWildcardExpansion() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX").state(State.OPEN))
            .put(indexBuilder("testXXY").state(State.OPEN))
            .put(indexBuilder("testXYY").state(State.CLOSE))
            .put(indexBuilder("testYYY").state(State.OPEN))
            .put(indexBuilder("testYYX").state(State.OPEN));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, false, false),
            SystemIndexAccessLevel.NONE
        );
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testX*")), equalTo(new HashSet<String>()));
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, true, false),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testX*")),
            equalTo(newHashSet("testXXX", "testXXY"))
        );
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, false, true),
            SystemIndexAccessLevel.NONE
        );
        assertThat(newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testX*")), equalTo(newHashSet("testXYY")));
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, true, true),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testX*")),
            equalTo(newHashSet("testXXX", "testXXY", "testXYY"))
        );
    }

    public void testConcreteIndicesWildcardWithNegation() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("testXXX").state(State.OPEN))
            .put(indexBuilder("testXXY").state(State.OPEN))
            .put(indexBuilder("testXYY").state(State.OPEN))
            .put(indexBuilder("-testXYZ").state(State.OPEN))
            .put(indexBuilder("-testXZZ").state(State.OPEN))
            .put(indexBuilder("-testYYY").state(State.OPEN))
            .put(indexBuilder("testYYY").state(State.OPEN))
            .put(indexBuilder("testYYX").state(State.OPEN));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(true, true, true, true),
            SystemIndexAccessLevel.NONE
        );
        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testX*")),
            equalTo(newHashSet("testXXX", "testXXY", "testXYY"))
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "test*", "-testX*")),
            equalTo(newHashSet("testYYY", "testYYX"))
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "-testX*")),
            equalTo(newHashSet("-testXYZ", "-testXZZ"))
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testXXY", "-testX*")),
            equalTo(newHashSet("testXXY", "-testXYZ", "-testXZZ"))
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "*", "--testX*")),
            equalTo(newHashSet("testXXX", "testXXY", "testXYY", "testYYX", "testYYY", "-testYYY"))
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "-testXXX", "test*")),
            equalTo(newHashSet("testYYX", "testXXX", "testXYY", "testYYY", "testXXY"))
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "test*", "-testXXX")),
            equalTo(newHashSet("testYYX", "testXYY", "testYYY", "testXXY"))
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testXXX", "testXXY", "testYYY", "-testYYY")),
            equalTo(newHashSet("testXXX", "testXXY", "testYYY", "-testYYY"))
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "testYYY", "testYYX", "testX*", "-testXXX")),
            equalTo(newHashSet("testYYY", "testYYX", "testXXY", "testXYY"))
        );

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(context, "-testXXX", "*testY*", "-testYYY")),
            equalTo(newHashSet("testYYX", "testYYY", "-testYYY"))
        );

        String[] indexNames = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), "-doesnotexist");
        assertEquals(0, indexNames.length);

        assertThat(
            newHashSet(indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), "-*")),
            equalTo(newHashSet("-testXYZ", "-testXZZ", "-testYYY"))
        );

        assertThat(
            newHashSet(
                indexNameExpressionResolver.concreteIndexNames(
                    state,
                    IndicesOptions.lenientExpandOpen(),
                    "testXXX",
                    "testXXY",
                    "testXYY",
                    "-testXXY"
                )
            ),
            equalTo(newHashSet("testXXX", "testXYY", "testXXY"))
        );

        indexNames = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), "*", "-*");
        assertEquals(0, indexNames.length);
    }

    public void testConcreteIndicesWildcardAndAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("foo_foo").state(State.OPEN).putAlias(AliasMetadata.builder("foo")))
            .put(indexBuilder("bar_bar").state(State.OPEN).putAlias(AliasMetadata.builder("foo")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        // when ignoreAliases option is set, concreteIndexNames resolves the provided expressions
        // only against the defined indices
        IndicesOptions ignoreAliasesOptions = IndicesOptions.fromOptions(false, false, true, false, true, false, true, false);

        String[] indexNamesIndexWildcard = indexNameExpressionResolver.concreteIndexNames(state, ignoreAliasesOptions, "foo*");

        assertEquals(1, indexNamesIndexWildcard.length);
        assertEquals("foo_foo", indexNamesIndexWildcard[0]);

        indexNamesIndexWildcard = indexNameExpressionResolver.concreteIndexNames(state, ignoreAliasesOptions, "*o");

        assertEquals(1, indexNamesIndexWildcard.length);
        assertEquals("foo_foo", indexNamesIndexWildcard[0]);

        indexNamesIndexWildcard = indexNameExpressionResolver.concreteIndexNames(state, ignoreAliasesOptions, "f*o");

        assertEquals(1, indexNamesIndexWildcard.length);
        assertEquals("foo_foo", indexNamesIndexWildcard[0]);

        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> indexNameExpressionResolver.concreteIndexNames(state, ignoreAliasesOptions, "foo")
        );
        assertEquals(
            "The provided expression [foo] matches an alias, specify the corresponding concrete indices instead.",
            iae.getMessage()
        );

        // when ignoreAliases option is not set, concreteIndexNames resolves the provided
        // expressions against the defined indices and aliases
        IndicesOptions indicesAndAliasesOptions = IndicesOptions.fromOptions(false, false, true, false, true, false, false, false);

        List<String> indexNames = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(state, indicesAndAliasesOptions, "foo*"));
        assertEquals(2, indexNames.size());
        assertTrue(indexNames.contains("foo_foo"));
        assertTrue(indexNames.contains("bar_bar"));

        indexNames = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(state, indicesAndAliasesOptions, "*o"));
        assertEquals(2, indexNames.size());
        assertTrue(indexNames.contains("foo_foo"));
        assertTrue(indexNames.contains("bar_bar"));

        indexNames = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(state, indicesAndAliasesOptions, "f*o"));
        assertEquals(2, indexNames.size());
        assertTrue(indexNames.contains("foo_foo"));
        assertTrue(indexNames.contains("bar_bar"));

        indexNames = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(state, indicesAndAliasesOptions, "foo"));
        assertEquals(2, indexNames.size());
        assertTrue(indexNames.contains("foo_foo"));
        assertTrue(indexNames.contains("bar_bar"));
    }

    public void testHiddenAliasAndHiddenIndexResolution() {
        final String visibleIndex = "visible_index";
        final String hiddenIndex = "hidden_index";
        final String visibleAlias = "visible_alias";
        final String hiddenAlias = "hidden_alias";
        final String dottedHiddenAlias = ".hidden_alias";
        final String dottedHiddenIndex = ".hidden_index";

        IndicesOptions excludeHiddenOptions = IndicesOptions.fromOptions(false, false, true, false, false, true, false, false, false);
        IndicesOptions includeHiddenOptions = IndicesOptions.fromOptions(false, false, true, false, true, true, false, false, false);

        {
            // A visible index with a visible alias and a hidden index with a hidden alias
            Metadata.Builder mdBuilder = Metadata.builder()
                .put(indexBuilder(visibleIndex).state(State.OPEN).putAlias(AliasMetadata.builder(visibleAlias)))
                .put(
                    indexBuilder(hiddenIndex, Settings.builder().put(INDEX_HIDDEN_SETTING.getKey(), true).build()).state(State.OPEN)
                        .putAlias(AliasMetadata.builder(hiddenAlias).isHidden(true))
                );
            ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

            // A total wildcard should only be resolved to visible indices
            String[] indexNames;
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "*");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex));

            // Unless hidden is specified in the options
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, "*");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));

            // Both hidden indices and hidden aliases should not be included in wildcard resolution
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "hidden*", "visible*");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex));

            // unless it's specified in the options
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, "hidden*", "visible*");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));

            // Only visible aliases should be included in wildcard resolution
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "*_alias");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex));

            // unless, again, it's specified in the options
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, "*_alias");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));

            // If we specify a hidden alias by name, the options shouldn't matter.
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, hiddenAlias);
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(hiddenIndex));

            indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, hiddenAlias);
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(hiddenIndex));
        }

        {
            // A visible alias that points to one hidden and one visible index
            Metadata.Builder mdBuilder = Metadata.builder()
                .put(indexBuilder(visibleIndex).state(State.OPEN).putAlias(AliasMetadata.builder(visibleAlias)))
                .put(
                    indexBuilder(hiddenIndex, Settings.builder().put(INDEX_HIDDEN_SETTING.getKey(), true).build()).state(State.OPEN)
                        .putAlias(AliasMetadata.builder(visibleAlias))
                );
            ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

            // If the alias is resolved to concrete indices, it should resolve to all the indices it points to, hidden or not.
            String[] indexNames;
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "*_alias");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, "*_alias");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, visibleAlias);
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, visibleAlias);
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));

            // A total wildcards does not resolve the hidden index in this case
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "*");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex));
        }

        {
            // A hidden alias that points to one hidden and one visible index
            Metadata.Builder mdBuilder = Metadata.builder()
                .put(indexBuilder(visibleIndex).state(State.OPEN).putAlias(AliasMetadata.builder(hiddenAlias).isHidden(true)))
                .put(
                    indexBuilder(hiddenIndex, Settings.builder().put(INDEX_HIDDEN_SETTING.getKey(), true).build()).state(State.OPEN)
                        .putAlias(AliasMetadata.builder(hiddenAlias).isHidden(true))
                );
            ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

            String[] indexNames;
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "*");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex));
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, "*");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));

            // A query that only matches the hidden alias should throw
            expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "*_alias")
            );

            // But if we include hidden it should be resolved to both indices
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, "*_alias");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));

            // If we specify the alias by name it should resolve to both indices, regardless of if the options specify hidden
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, hiddenAlias);
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, hiddenAlias);
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(visibleIndex, hiddenIndex));
        }

        {
            // A hidden alias with a dot-prefixed name that points to one hidden index with a dot prefix, and one hidden index without
            Metadata.Builder mdBuilder = Metadata.builder()
                .put(
                    indexBuilder(dottedHiddenIndex, Settings.builder().put(INDEX_HIDDEN_SETTING.getKey(), true).build()).state(State.OPEN)
                        .putAlias(AliasMetadata.builder(dottedHiddenAlias).isHidden(true))
                )
                .put(
                    indexBuilder(hiddenIndex, Settings.builder().put(INDEX_HIDDEN_SETTING.getKey(), true).build()).state(State.OPEN)
                        .putAlias(AliasMetadata.builder(dottedHiddenAlias).isHidden(true))
                );
            ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

            String[] indexNames;
            // A dot-prefixed pattern that includes only the hidden alias should resolve to both, regardless of the options
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, ".hidden_a*");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(dottedHiddenIndex, hiddenIndex));
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, ".hidden_a*");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(dottedHiddenIndex, hiddenIndex));

            // A query that doesn't include the dot should fail if the options don't include hidden
            expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "*_alias")
            );

            // But should include both indices if the options do include hidden
            indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, "*_alias");
            assertThat(Arrays.asList(indexNames), containsInAnyOrder(dottedHiddenIndex, hiddenIndex));

        }
    }

    public void testHiddenIndexWithVisibleAliasOverlappingNameResolution() {
        final String hiddenIndex = "my-hidden-index";
        final String hiddenAlias = "my-hidden-alias";
        final String visibleAlias = "my-visible-alias";

        IndicesOptions excludeHiddenOptions = IndicesOptions.fromOptions(false, true, true, false, false, true, false, false, false);
        IndicesOptions includeHiddenOptions = IndicesOptions.fromOptions(false, true, true, false, true, true, false, false, false);

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                indexBuilder(hiddenIndex, Settings.builder().put(INDEX_HIDDEN_SETTING.getKey(), true).build()).state(State.OPEN)
                    .putAlias(AliasMetadata.builder(hiddenAlias).isHidden(true))
                    .putAlias(AliasMetadata.builder(visibleAlias).build())
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        String[] indexNames;
        indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "my-*");
        assertThat(Arrays.asList(indexNames), containsInAnyOrder(hiddenIndex));

        indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "my-hidden*");
        assertThat(Arrays.asList(indexNames), empty());
        indexNames = indexNameExpressionResolver.concreteIndexNames(state, excludeHiddenOptions, "my-*", "-my-visible*");
        assertThat(Arrays.asList(indexNames), empty());
        indexNames = indexNameExpressionResolver.concreteIndexNames(state, includeHiddenOptions, "my-hidden*", "-my-hidden-a*");
        assertThat(Arrays.asList(indexNames), empty());
    }

    /**
     * test resolving _all pattern (null, empty array or "_all") for random IndicesOptions
     */
    public void testConcreteIndicesAllPatternRandom() {
        for (int i = 0; i < 10; i++) {
            final String[] allIndices;
            switch (randomIntBetween(0, 2)) {
                case 0:
                    allIndices = null;
                    break;
                case 1:
                    allIndices = new String[0];
                    break;
                case 2:
                    allIndices = new String[] { Metadata.ALL };
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            final IndicesOptions indicesOptions = IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );

            {
                ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(Metadata.builder().build()).build();
                IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                    state,
                    indicesOptions,
                    SystemIndexAccessLevel.NONE
                );

                // with no indices, asking for all indices should return empty list or exception, depending on indices options
                if (indicesOptions.allowNoIndices()) {
                    String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(context, allIndices);
                    assertThat(concreteIndices, notNullValue());
                    assertThat(concreteIndices.length, equalTo(0));
                } else {
                    expectThrows(IndexNotFoundException.class, () -> indexNameExpressionResolver.concreteIndexNames(context, allIndices));
                }
            }

            {
                // with existing indices, asking for all indices should return all open/closed indices depending on options
                Metadata.Builder mdBuilder = Metadata.builder()
                    .put(indexBuilder("aaa").state(State.OPEN).putAlias(AliasMetadata.builder("aaa_alias1")))
                    .put(indexBuilder("bbb").state(State.OPEN).putAlias(AliasMetadata.builder("bbb_alias1")))
                    .put(indexBuilder("ccc").state(State.CLOSE).putAlias(AliasMetadata.builder("ccc_alias1")));
                ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
                IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                    state,
                    indicesOptions,
                    SystemIndexAccessLevel.NONE
                );
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
                    expectThrows(IndexNotFoundException.class, () -> indexNameExpressionResolver.concreteIndexNames(context, allIndices));
                }
            }
        }
    }

    /**
     * test resolving wildcard pattern that matches no index of alias for random IndicesOptions
     */
    public void testConcreteIndicesWildcardNoMatch() {
        for (int i = 0; i < 10; i++) {
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
            Metadata.Builder mdBuilder = Metadata.builder()
                .put(indexBuilder("aaa").state(State.OPEN).putAlias(AliasMetadata.builder("aaa_alias1")))
                .put(indexBuilder("bbb").state(State.OPEN).putAlias(AliasMetadata.builder("bbb_alias1")))
                .put(indexBuilder("ccc").state(State.CLOSE).putAlias(AliasMetadata.builder("ccc_alias1")));
            ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                indicesOptions,
                SystemIndexAccessLevel.NONE
            );

            // asking for non existing wildcard pattern should return empty list or exception
            if (indicesOptions.allowNoIndices()) {
                String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(context, "Foo*");
                assertThat(concreteIndices, notNullValue());
                assertThat(concreteIndices.length, equalTo(0));
            } else {
                expectThrows(IndexNotFoundException.class, () -> indexNameExpressionResolver.concreteIndexNames(context, "Foo*"));
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
        // even though it does identify all indices, it's not a pattern but just an explicit list of them
        String[] concreteIndices = new String[] { "index1", "index2", "index3" };
        Metadata metadata = metadataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metadata, concreteIndices, concreteIndices), equalTo(false));
    }

    public void testIsPatternMatchingAllIndicesOnlyWildcard() throws Exception {
        String[] indicesOrAliases = new String[] { "*" };
        String[] concreteIndices = new String[] { "index1", "index2", "index3" };
        Metadata metadata = metadataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metadata, indicesOrAliases, concreteIndices), equalTo(true));
    }

    public void testIsPatternMatchingAllIndicesMatchingTrailingWildcard() throws Exception {
        String[] indicesOrAliases = new String[] { "index*" };
        String[] concreteIndices = new String[] { "index1", "index2", "index3" };
        Metadata metadata = metadataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metadata, indicesOrAliases, concreteIndices), equalTo(true));
    }

    public void testIsPatternMatchingAllIndicesNonMatchingTrailingWildcard() throws Exception {
        String[] indicesOrAliases = new String[] { "index*" };
        String[] concreteIndices = new String[] { "index1", "index2", "index3" };
        String[] allConcreteIndices = new String[] { "index1", "index2", "index3", "a", "b" };
        Metadata metadata = metadataBuilder(allConcreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metadata, indicesOrAliases, concreteIndices), equalTo(false));
    }

    public void testIsPatternMatchingAllIndicesMatchingSingleExclusion() throws Exception {
        String[] indicesOrAliases = new String[] { "-index1", "index1" };
        String[] concreteIndices = new String[] { "index1", "index2", "index3" };
        Metadata metadata = metadataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metadata, indicesOrAliases, concreteIndices), equalTo(true));
    }

    public void testIsPatternMatchingAllIndicesNonMatchingSingleExclusion() throws Exception {
        String[] indicesOrAliases = new String[] { "-index1" };
        String[] concreteIndices = new String[] { "index2", "index3" };
        String[] allConcreteIndices = new String[] { "index1", "index2", "index3" };
        Metadata metadata = metadataBuilder(allConcreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metadata, indicesOrAliases, concreteIndices), equalTo(false));
    }

    public void testIsPatternMatchingAllIndicesMatchingTrailingWildcardAndExclusion() throws Exception {
        String[] indicesOrAliases = new String[] { "index*", "-index1", "index1" };
        String[] concreteIndices = new String[] { "index1", "index2", "index3" };
        Metadata metadata = metadataBuilder(concreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metadata, indicesOrAliases, concreteIndices), equalTo(true));
    }

    public void testIsPatternMatchingAllIndicesNonMatchingTrailingWildcardAndExclusion() throws Exception {
        String[] indicesOrAliases = new String[] { "index*", "-index1" };
        String[] concreteIndices = new String[] { "index2", "index3" };
        String[] allConcreteIndices = new String[] { "index1", "index2", "index3" };
        Metadata metadata = metadataBuilder(allConcreteIndices);
        assertThat(indexNameExpressionResolver.isPatternMatchingAllIndices(metadata, indicesOrAliases, concreteIndices), equalTo(false));
    }

    public void testIndexOptionsFailClosedIndicesAndAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                indexBuilder("foo1-closed").state(IndexMetadata.State.CLOSE)
                    .putAlias(AliasMetadata.builder("foobar1-closed"))
                    .putAlias(AliasMetadata.builder("foobar2-closed"))
            )
            .put(indexBuilder("foo2-closed").state(IndexMetadata.State.CLOSE).putAlias(AliasMetadata.builder("foobar2-closed")))
            .put(indexBuilder("foo3").putAlias(AliasMetadata.builder("foobar2-closed")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context contextICE = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.strictExpandOpenAndForbidClosed(),
            SystemIndexAccessLevel.NONE
        );
        expectThrows(IndexClosedException.class, () -> indexNameExpressionResolver.concreteIndexNames(contextICE, "foo1-closed"));
        expectThrows(IndexClosedException.class, () -> indexNameExpressionResolver.concreteIndexNames(contextICE, "foobar1-closed"));

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(
                true,
                contextICE.getOptions().allowNoIndices(),
                contextICE.getOptions().expandWildcardsOpen(),
                contextICE.getOptions().expandWildcardsClosed(),
                contextICE.getOptions()
            ),
            SystemIndexAccessLevel.NONE
        );
        String[] results = indexNameExpressionResolver.concreteIndexNames(context, "foo1-closed");
        assertThat(results, emptyArray());

        results = indexNameExpressionResolver.concreteIndexNames(context, "foobar1-closed");
        assertThat(results, emptyArray());

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen(), SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, "foo1-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo1-closed"));

        results = indexNameExpressionResolver.concreteIndexNames(context, "foobar1-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo1-closed"));

        // testing an alias pointing to three indices:
        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.strictExpandOpenAndForbidClosed(),
            SystemIndexAccessLevel.NONE
        );
        try {
            indexNameExpressionResolver.concreteIndexNames(context, "foobar2-closed");
            fail("foo2-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.fromOptions(
                true,
                context.getOptions().allowNoIndices(),
                context.getOptions().expandWildcardsOpen(),
                context.getOptions().expandWildcardsClosed(),
                context.getOptions()
            ),
            SystemIndexAccessLevel.NONE
        );
        results = indexNameExpressionResolver.concreteIndexNames(context, "foobar2-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo3"));

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen(), SystemIndexAccessLevel.NONE);
        results = indexNameExpressionResolver.concreteIndexNames(context, "foobar2-closed");
        assertThat(results, arrayWithSize(3));
        assertThat(results, arrayContainingInAnyOrder("foo1-closed", "foo2-closed", "foo3"));
    }

    public void testDedupConcreteIndices() {
        Metadata.Builder mdBuilder = Metadata.builder().put(indexBuilder("index1").putAlias(AliasMetadata.builder("alias1")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndicesOptions[] indicesOptions = new IndicesOptions[] {
            IndicesOptions.strictExpandOpen(),
            IndicesOptions.strictExpand(),
            IndicesOptions.lenientExpandOpen(),
            IndicesOptions.strictExpandOpenAndForbidClosed() };
        for (IndicesOptions options : indicesOptions) {
            IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
                state,
                options,
                SystemIndexAccessLevel.NONE
            );
            String[] results = indexNameExpressionResolver.concreteIndexNames(context, "index1", "index1", "alias1");
            assertThat(results, equalTo(new String[] { "index1" }));
        }
    }

    private static Metadata metadataBuilder(String... indices) {
        Metadata.Builder mdBuilder = Metadata.builder();
        for (String concreteIndex : indices) {
            mdBuilder.put(indexBuilder(concreteIndex));
        }
        return mdBuilder.build();
    }

    public void testFilterClosedIndicesOnAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-0").state(State.OPEN).putAlias(AliasMetadata.builder("alias-0")))
            .put(indexBuilder("test-1").state(IndexMetadata.State.CLOSE).putAlias(AliasMetadata.builder("alias-1")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );
        String[] strings = indexNameExpressionResolver.concreteIndexNames(context, "alias-*");
        assertArrayEquals(new String[] { "test-0" }, strings);

        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.strictExpandOpen(), SystemIndexAccessLevel.NONE);
        strings = indexNameExpressionResolver.concreteIndexNames(context, "alias-*");

        assertArrayEquals(new String[] { "test-0" }, strings);
    }

    public void testResolveExpressions() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-0").state(State.OPEN).putAlias(AliasMetadata.builder("alias-0").filter("{ \"term\": \"foo\"}")))
            .put(indexBuilder("test-1").state(State.OPEN).putAlias(AliasMetadata.builder("alias-1")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        assertEquals(new HashSet<>(Arrays.asList("alias-0", "alias-1")), indexNameExpressionResolver.resolveExpressions(state, "alias-*"));
        assertEquals(
            new HashSet<>(Arrays.asList("test-0", "alias-0", "alias-1")),
            indexNameExpressionResolver.resolveExpressions(state, "test-0", "alias-*")
        );
        assertEquals(
            new HashSet<>(Arrays.asList("test-0", "test-1", "alias-0", "alias-1")),
            indexNameExpressionResolver.resolveExpressions(state, "test-*", "alias-*")
        );
        assertEquals(new HashSet<>(Arrays.asList("test-1", "alias-1")), indexNameExpressionResolver.resolveExpressions(state, "*-1"));
    }

    public void testFilteringAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-0").state(State.OPEN).putAlias(AliasMetadata.builder("alias-0").filter("{ \"term\": \"foo\"}")))
            .put(indexBuilder("test-1").state(State.OPEN).putAlias(AliasMetadata.builder("alias-1")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        Set<String> resolvedExpressions = new HashSet<>(Arrays.asList("alias-0", "alias-1"));
        String[] strings = indexNameExpressionResolver.filteringAliases(state, "test-0", resolvedExpressions);
        assertArrayEquals(new String[] { "alias-0" }, strings);

        // concrete index supersedes filtering alias
        resolvedExpressions = new HashSet<>(Arrays.asList("test-0", "alias-0", "alias-1"));
        strings = indexNameExpressionResolver.filteringAliases(state, "test-0", resolvedExpressions);
        assertNull(strings);

        resolvedExpressions = new HashSet<>(Arrays.asList("test-0", "test-1", "alias-0", "alias-1"));
        strings = indexNameExpressionResolver.filteringAliases(state, "test-0", resolvedExpressions);
        assertNull(strings);
    }

    public void testIndexAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                indexBuilder("test-0").state(State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias-0").filter("{ \"term\": \"foo\"}"))
                    .putAlias(AliasMetadata.builder("test-alias-1").filter("{ \"term\": \"foo\"}"))
                    .putAlias(AliasMetadata.builder("test-alias-non-filtering"))
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        Set<String> resolvedExpressions = indexNameExpressionResolver.resolveExpressions(state, "test-*");

        String[] strings = indexNameExpressionResolver.indexAliases(state, "test-0", x -> true, x -> true, true, resolvedExpressions);
        Arrays.sort(strings);
        assertArrayEquals(new String[] { "test-alias-0", "test-alias-1", "test-alias-non-filtering" }, strings);

        strings = indexNameExpressionResolver.indexAliases(
            state,
            "test-0",
            x -> x.alias().equals("test-alias-1"),
            x -> false,
            true,
            resolvedExpressions
        );
        assertArrayEquals(null, strings);
    }

    public void testIndexAliasesDataStreamAliases() {
        final String dataStreamName1 = "logs-foobar";
        final String dataStreamName2 = "logs-barbaz";
        IndexMetadata backingIndex1 = createBackingIndex(dataStreamName1, 1).build();
        IndexMetadata backingIndex2 = createBackingIndex(dataStreamName2, 1).build();
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(backingIndex1, false)
            .put(backingIndex2, false)
            .put(newInstance(dataStreamName1, createTimestampField("@timestamp"), Collections.singletonList(backingIndex1.getIndex())))
            .put(newInstance(dataStreamName2, createTimestampField("@timestamp"), Collections.singletonList(backingIndex2.getIndex())));
        mdBuilder.put("logs_foo", dataStreamName1, null, "{ \"term\": \"foo\"}");
        mdBuilder.put("logs", dataStreamName1, null, "{ \"term\": \"logs\"}");
        mdBuilder.put("logs_bar", dataStreamName1, null, null);
        mdBuilder.put("logs_baz", dataStreamName2, null, "{ \"term\": \"logs\"}");
        mdBuilder.put("logs_baz2", dataStreamName2, null, null);
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        {
            // Only resolve aliases with with that refer to dataStreamName1
            Set<String> resolvedExpressions = indexNameExpressionResolver.resolveExpressions(state, "l*");
            String index = backingIndex1.getIndex().getName();
            String[] result = indexNameExpressionResolver.indexAliases(state, index, x -> true, x -> true, true, resolvedExpressions);
            assertThat(result, arrayContainingInAnyOrder("logs_foo", "logs", "logs_bar"));
        }
        {
            // Only resolve aliases with with that refer to dataStreamName2
            Set<String> resolvedExpressions = indexNameExpressionResolver.resolveExpressions(state, "l*");
            String index = backingIndex2.getIndex().getName();
            String[] result = indexNameExpressionResolver.indexAliases(state, index, x -> true, x -> true, true, resolvedExpressions);
            assertThat(result, arrayContainingInAnyOrder("logs_baz", "logs_baz2"));
        }
        {
            // Null is returned, because skipping identity check and resolvedExpressions contains the backing index name
            Set<String> resolvedExpressions = indexNameExpressionResolver.resolveExpressions(state, "l*");
            String index = backingIndex2.getIndex().getName();
            String[] result = indexNameExpressionResolver.indexAliases(state, index, x -> true, x -> true, false, resolvedExpressions);
            assertThat(result, nullValue());
        }
    }

    public void testIndexAliasesSkipIdentity() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                indexBuilder("test-0").state(State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias"))
                    .putAlias(AliasMetadata.builder("other-alias"))
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        Set<String> resolvedExpressions = new HashSet<>(Arrays.asList("test-0", "test-alias"));
        String[] aliases = indexNameExpressionResolver.indexAliases(state, "test-0", x -> true, x -> true, false, resolvedExpressions);
        assertNull(aliases);
        aliases = indexNameExpressionResolver.indexAliases(state, "test-0", x -> true, x -> true, true, resolvedExpressions);
        assertArrayEquals(new String[] { "test-alias" }, aliases);

        resolvedExpressions = Collections.singleton("other-alias");
        aliases = indexNameExpressionResolver.indexAliases(state, "test-0", x -> true, x -> true, false, resolvedExpressions);
        assertArrayEquals(new String[] { "other-alias" }, aliases);
        aliases = indexNameExpressionResolver.indexAliases(state, "test-0", x -> true, x -> true, true, resolvedExpressions);
        assertArrayEquals(new String[] { "other-alias" }, aliases);
    }

    public void testConcreteWriteIndexSuccessful() {
        boolean testZeroWriteIndex = randomBoolean();
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                indexBuilder("test-0").state(State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias").writeIndex(testZeroWriteIndex ? true : null))
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        String[] strings = indexNameExpressionResolver.indexAliases(
            state,
            "test-0",
            x -> true,
            x -> true,
            true,
            new HashSet<>(Arrays.asList("test-0", "test-alias"))
        );
        Arrays.sort(strings);
        assertArrayEquals(new String[] { "test-alias" }, strings);
        IndicesRequest request = new IndicesRequest() {

            @Override
            public String[] indices() {
                return new String[] { "test-alias" };
            }

            @Override
            public IndicesOptions indicesOptions() {
                return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
            }

            @Override
            public boolean includeDataStreams() {
                return false;
            }
        };
        Index writeIndex = indexNameExpressionResolver.concreteWriteIndex(state, request);
        assertThat(writeIndex.getName(), equalTo("test-0"));

        state = ClusterState.builder(state)
            .metadata(
                Metadata.builder(state.metadata())
                    .put(
                        indexBuilder("test-1").putAlias(
                            AliasMetadata.builder("test-alias").writeIndex(testZeroWriteIndex ? randomFrom(false, null) : true)
                        )
                    )
            )
            .build();
        writeIndex = indexNameExpressionResolver.concreteWriteIndex(state, request);
        assertThat(writeIndex.getName(), equalTo(testZeroWriteIndex ? "test-0" : "test-1"));
    }

    public void testConcreteWriteIndexWithInvalidIndicesRequest() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-0").state(State.OPEN).putAlias(AliasMetadata.builder("test-alias")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        Function<String[], IndicesRequest> requestGen = (indices) -> new IndicesRequest() {

            @Override
            public String[] indices() {
                return indices;
            }

            @Override
            public IndicesOptions indicesOptions() {
                return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
            }

            @Override
            public boolean includeDataStreams() {
                return false;
            }
        };
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indexNameExpressionResolver.concreteWriteIndex(state, requestGen.apply(null))
        );
        assertThat(exception.getMessage(), equalTo("indices request must specify a single index expression"));
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> indexNameExpressionResolver.concreteWriteIndex(state, requestGen.apply(new String[] { "too", "many" }))
        );
        assertThat(exception.getMessage(), equalTo("indices request must specify a single index expression"));

    }

    public void testConcreteWriteIndexWithWildcardExpansion() {
        boolean testZeroWriteIndex = randomBoolean();
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                indexBuilder("test-1").state(State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias").writeIndex(testZeroWriteIndex ? true : null))
            )
            .put(
                indexBuilder("test-0").state(State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias").writeIndex(testZeroWriteIndex ? randomFrom(false, null) : true))
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        String[] strings = indexNameExpressionResolver.indexAliases(
            state,
            "test-0",
            x -> true,
            x -> true,
            true,
            new HashSet<>(Arrays.asList("test-0", "test-1", "test-alias"))
        );
        Arrays.sort(strings);
        assertArrayEquals(new String[] { "test-alias" }, strings);
        IndicesRequest request = new IndicesRequest() {

            @Override
            public String[] indices() {
                return new String[] { "test-*" };
            }

            @Override
            public IndicesOptions indicesOptions() {
                return IndicesOptions.strictExpandOpenAndForbidClosed();
            }

            @Override
            public boolean includeDataStreams() {
                return false;
            }
        };

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indexNameExpressionResolver.concreteWriteIndex(state, request)
        );
        assertThat(
            exception.getMessage(),
            equalTo("The index expression [test-*] and options provided did not point to a single write-index")
        );
    }

    public void testConcreteWriteIndexWithNoWriteIndexWithSingleIndex() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-0").state(State.OPEN).putAlias(AliasMetadata.builder("test-alias").writeIndex(false)));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        String[] strings = indexNameExpressionResolver.indexAliases(
            state,
            "test-0",
            x -> true,
            x -> true,
            true,
            new HashSet<>(Arrays.asList("test-0", "test-alias"))
        );
        Arrays.sort(strings);
        assertArrayEquals(new String[] { "test-alias" }, strings);
        DocWriteRequest<?> request = randomFrom(
            new IndexRequest("test-alias"),
            new UpdateRequest("test-alias", "_type", "_id"),
            new DeleteRequest("test-alias")
        );
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indexNameExpressionResolver.concreteWriteIndex(state, request.indicesOptions(), request.indices()[0], false, false)
        );
        assertThat(
            exception.getMessage(),
            equalTo(
                "no write index is defined for alias [test-alias]."
                    + " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple"
                    + " indices without one being designated as a write index"
            )
        );
    }

    public void testConcreteWriteIndexWithNoWriteIndexWithMultipleIndices() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-0").state(State.OPEN).putAlias(AliasMetadata.builder("test-alias").writeIndex(randomFrom(false, null))))
            .put(
                indexBuilder("test-1").state(State.OPEN).putAlias(AliasMetadata.builder("test-alias").writeIndex(randomFrom(false, null)))
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        String[] strings = indexNameExpressionResolver.indexAliases(
            state,
            "test-0",
            x -> true,
            x -> true,
            true,
            new HashSet<>(Arrays.asList("test-0", "test-1", "test-alias"))
        );
        Arrays.sort(strings);
        assertArrayEquals(new String[] { "test-alias" }, strings);
        DocWriteRequest<?> request = randomFrom(
            new IndexRequest("test-alias"),
            new UpdateRequest("test-alias", "_type", "_id"),
            new DeleteRequest("test-alias")
        );
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indexNameExpressionResolver.concreteWriteIndex(state, request.indicesOptions(), request.indices()[0], false, false)
        );
        assertThat(
            exception.getMessage(),
            equalTo(
                "no write index is defined for alias [test-alias]."
                    + " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple"
                    + " indices without one being designated as a write index"
            )
        );
    }

    public void testAliasResolutionNotAllowingMultipleIndices() {
        boolean test0WriteIndex = randomBoolean();
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                indexBuilder("test-0").state(State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias").writeIndex(randomFrom(test0WriteIndex, null)))
            )
            .put(
                indexBuilder("test-1").state(State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias").writeIndex(randomFrom(test0WriteIndex == false, null)))
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        String[] strings = indexNameExpressionResolver.indexAliases(
            state,
            "test-0",
            x -> true,
            x -> true,
            true,
            new HashSet<>(Arrays.asList("test-0", "test-1", "test-alias"))
        );
        Arrays.sort(strings);
        assertArrayEquals(new String[] { "test-alias" }, strings);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indexNameExpressionResolver.concreteIndexNames(
                state,
                IndicesOptions.strictSingleIndexNoExpandForbidClosed(),
                "test-alias"
            )
        );
        assertThat(exception.getMessage(), endsWith(", can't execute a single index op"));
    }

    public void testDeleteIndexIgnoresAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-index").state(State.OPEN).putAlias(AliasMetadata.builder("test-alias")))
            .put(indexBuilder("index").state(State.OPEN).putAlias(AliasMetadata.builder("test-alias2")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        {
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, new DeleteIndexRequest("does_not_exist"))
            );
            assertEquals("does_not_exist", infe.getIndex().getName());
            assertEquals("no such index [does_not_exist]", infe.getMessage());
        }
        {
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, new DeleteIndexRequest("test-alias"))
            );
            assertEquals(
                "The provided expression [test-alias] matches an alias, " + "specify the corresponding concrete indices instead.",
                iae.getMessage()
            );
        }
        {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("test-alias");
            deleteIndexRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, true, false, false, true, false));
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, deleteIndexRequest);
            assertEquals(0, indices.length);
        }
        {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("test-a*");
            deleteIndexRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, true, false, false, true, false));
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, deleteIndexRequest)
            );
            assertEquals(infe.getIndex().getName(), "test-a*");
        }
        {
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, new DeleteIndexRequest("test-a*"));
            assertEquals(0, indices.length);
        }
        {
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, new DeleteIndexRequest("test-index"));
            assertEquals(1, indices.length);
            assertEquals("test-index", indices[0]);
        }
        {
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, new DeleteIndexRequest("test-*"));
            assertEquals(1, indices.length);
            assertEquals("test-index", indices[0]);
        }
    }

    public void testIndicesAliasesRequestIgnoresAliases() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-index").state(State.OPEN).putAlias(AliasMetadata.builder("test-alias")))
            .put(indexBuilder("index").state(State.OPEN).putAlias(AliasMetadata.builder("test-alias2")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.add().index("test-alias");
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, aliasActions)
            );
            assertEquals(
                "The provided expression [test-alias] matches an alias, " + "specify the corresponding concrete indices instead.",
                iae.getMessage()
            );
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.add().index("test-a*");
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, aliasActions)
            );
            assertEquals("test-a*", infe.getIndex().getName());
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.add().index("test-index");
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, aliasActions);
            assertEquals(1, indices.length);
            assertEquals("test-index", indices[0]);
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.add().index("test-*");
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, aliasActions);
            assertEquals(1, indices.length);
            assertEquals("test-index", indices[0]);
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.remove().index("test-alias");
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, aliasActions)
            );
            assertEquals(
                "The provided expression [test-alias] matches an alias, " + "specify the corresponding concrete indices instead.",
                iae.getMessage()
            );
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.remove().index("test-a*");
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, aliasActions)
            );
            assertEquals("test-a*", infe.getIndex().getName());
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.remove().index("test-index");
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, aliasActions);
            assertEquals(1, indices.length);
            assertEquals("test-index", indices[0]);
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.remove().index("test-*");
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, aliasActions);
            assertEquals(1, indices.length);
            assertEquals("test-index", indices[0]);
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.removeIndex().index("test-alias");
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, aliasActions)
            );
            assertEquals(
                "The provided expression [test-alias] matches an alias, " + "specify the corresponding concrete indices instead.",
                iae.getMessage()
            );
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.removeIndex().index("test-a*");
            IndexNotFoundException infe = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndexNames(state, aliasActions)
            );
            assertEquals("test-a*", infe.getIndex().getName());
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.removeIndex().index("test-index");
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, aliasActions);
            assertEquals(1, indices.length);
            assertEquals("test-index", indices[0]);
        }
        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.removeIndex().index("test-*");
            String[] indices = indexNameExpressionResolver.concreteIndexNames(state, aliasActions);
            assertEquals(1, indices.length);
            assertEquals("test-index", indices[0]);
        }
    }

    public void testIndicesAliasesRequestTargetDataStreams() {
        final String dataStreamName = "my-data-stream";
        IndexMetadata backingIndex = createBackingIndex(dataStreamName, 1).build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(backingIndex, false)
            .put(newInstance(dataStreamName, createTimestampField("@timestamp"), org.elasticsearch.core.List.of(backingIndex.getIndex())));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.add().index(dataStreamName);
            assertThat(
                indexNameExpressionResolver.concreteIndexNames(state, aliasActions),
                arrayContaining(backingIndexEqualTo(dataStreamName, 1))
            );
        }

        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.add().index("my-data-*").alias("my-data");
            assertThat(
                indexNameExpressionResolver.concreteIndexNames(state, aliasActions),
                arrayContaining(backingIndexEqualTo(dataStreamName, 1))
            );
        }

        {
            IndicesAliasesRequest.AliasActions aliasActions = IndicesAliasesRequest.AliasActions.add()
                .index(dataStreamName)
                .alias("my-data");
            assertThat(
                indexNameExpressionResolver.concreteIndexNames(state, aliasActions),
                arrayContaining(backingIndexEqualTo(dataStreamName, 1))
            );
        }
    }

    public void testInvalidIndex() {
        Metadata.Builder mdBuilder = Metadata.builder().put(indexBuilder("test"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            SystemIndexAccessLevel.NONE
        );

        InvalidIndexNameException iine = expectThrows(
            InvalidIndexNameException.class,
            () -> indexNameExpressionResolver.concreteIndexNames(context, "_foo")
        );
        assertEquals("Invalid index name [_foo], must not start with '_'.", iine.getMessage());
    }

    public void testIgnoreThrottled() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                indexBuilder("test-index", Settings.builder().put("index.frozen", true).build()).state(State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias"))
            )
            .put(
                indexBuilder("index", Settings.builder().put(IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), true).build()).state(State.OPEN)
                    .putAlias(AliasMetadata.builder("test-alias2"))
            )
            .put(
                indexBuilder("index-closed", Settings.builder().put("index.frozen", true).build()).state(State.CLOSE)
                    .putAlias(AliasMetadata.builder("test-alias-closed"))
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        {
            Index[] indices = indexNameExpressionResolver.concreteIndices(
                state,
                IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED,
                "*"
            );
            assertEquals(1, indices.length);
            assertEquals("index", indices[0].getName());
        }
        {
            Index[] indices = indexNameExpressionResolver.concreteIndices(
                state,
                IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED,
                "test-alias"
            );
            assertEquals(1, indices.length);
            assertEquals("test-index", indices[0].getName());
        }
        {
            Index[] indices = indexNameExpressionResolver.concreteIndices(
                state,
                IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED,
                "test-alias"
            );
            assertEquals(0, indices.length);
        }
        {
            Index[] indices = indexNameExpressionResolver.concreteIndices(
                state,
                IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED,
                "test-*"
            );
            assertEquals(1, indices.length);
            assertEquals("index", indices[0].getName());
        }
        {
            Index[] indices = indexNameExpressionResolver.concreteIndices(
                state,
                IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED,
                "ind*",
                "test-index"
            );
            assertEquals(1, indices.length);
            Arrays.sort(indices, Comparator.comparing(Index::getName));
            assertEquals("index", indices[0].getName());
        }

        {
            Index[] indices = indexNameExpressionResolver.concreteIndices(
                state,
                new IndicesOptions(
                    EnumSet.of(IndicesOptions.Option.ALLOW_NO_INDICES, IndicesOptions.Option.IGNORE_THROTTLED),
                    EnumSet.of(IndicesOptions.WildcardStates.OPEN)
                ),
                "ind*",
                "test-index"
            );
            assertEquals(1, indices.length);
            Arrays.sort(indices, Comparator.comparing(Index::getName));
            assertEquals("index", indices[0].getName());
        }
        {
            Index[] indices = indexNameExpressionResolver.concreteIndices(
                state,
                new IndicesOptions(
                    EnumSet.of(IndicesOptions.Option.ALLOW_NO_INDICES),
                    EnumSet.of(IndicesOptions.WildcardStates.OPEN, IndicesOptions.WildcardStates.CLOSED)
                ),
                "ind*",
                "test-index"
            );
            assertEquals(3, indices.length);
            Arrays.sort(indices, Comparator.comparing(Index::getName));
            assertEquals("index", indices[0].getName());
            assertEquals("index-closed", indices[1].getName());
            assertEquals("test-index", indices[2].getName());
        }
    }

    public void testFullWildcardSystemIndexResolutionAllowed() {
        ClusterState state = systemIndexTestClusterState();
        SearchRequest request = new SearchRequest(randomFrom("*", "_all"));

        List<String> indexNames = resolveConcreteIndexNameList(state, request);
        assertThat(indexNames, containsInAnyOrder("some-other-index", ".ml-stuff", ".ml-meta", ".watches"));
    }

    public void testWildcardSystemIndexResolutionMultipleMatchesAllowed() {
        ClusterState state = systemIndexTestClusterState();
        SearchRequest request = new SearchRequest(".w*");

        List<String> indexNames = resolveConcreteIndexNameList(state, request);
        assertThat(indexNames, containsInAnyOrder(".watches"));
    }

    public void testWildcardSystemIndexResolutionSingleMatchAllowed() {
        ClusterState state = systemIndexTestClusterState();
        SearchRequest request = new SearchRequest(".ml-*");

        List<String> indexNames = resolveConcreteIndexNameList(state, request);
        assertThat(indexNames, containsInAnyOrder(".ml-meta", ".ml-stuff"));
    }

    public void testSingleSystemIndexResolutionAllowed() {
        ClusterState state = systemIndexTestClusterState();
        SearchRequest request = new SearchRequest(".ml-meta");

        List<String> indexNames = resolveConcreteIndexNameList(state, request);
        assertThat(indexNames, containsInAnyOrder(".ml-meta"));
    }

    public void testFullWildcardSystemIndexResolutionDeprecated() {
        threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.FALSE.toString());
        ClusterState state = systemIndexTestClusterState();
        SearchRequest request = new SearchRequest(randomFrom("*", "_all"));

        List<String> indexNames = resolveConcreteIndexNameList(state, request);
        assertThat(indexNames, containsInAnyOrder("some-other-index", ".ml-stuff", ".ml-meta", ".watches"));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "this request accesses system indices: [.ml-meta, .ml-stuff, .watches], "
                    + "but in a future major version, direct access to system indices will be prevented by default"
            )
        );

    }

    public void testSingleSystemIndexResolutionDeprecated() {
        threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.FALSE.toString());
        ClusterState state = systemIndexTestClusterState();
        SearchRequest request = new SearchRequest(".ml-meta");

        List<String> indexNames = resolveConcreteIndexNameList(state, request);
        assertThat(indexNames, containsInAnyOrder(".ml-meta"));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "this request accesses system indices: [.ml-meta], "
                    + "but in a future major version, direct access to system indices will be prevented by default"
            )
        );
    }

    public void testWildcardSystemIndexReslutionSingleMatchDeprecated() {
        threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.FALSE.toString());
        ClusterState state = systemIndexTestClusterState();
        SearchRequest request = new SearchRequest(".w*");

        List<String> indexNames = resolveConcreteIndexNameList(state, request);
        assertThat(indexNames, containsInAnyOrder(".watches"));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "this request accesses system indices: [.watches], "
                    + "but in a future major version, direct access to system indices will be prevented by default"
            )
        );

    }

    public void testWildcardSystemIndexResolutionMultipleMatchesDeprecated() {
        threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.FALSE.toString());
        ClusterState state = systemIndexTestClusterState();
        SearchRequest request = new SearchRequest(".ml-*");

        List<String> indexNames = resolveConcreteIndexNameList(state, request);
        assertThat(indexNames, containsInAnyOrder(".ml-meta", ".ml-stuff"));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "this request accesses system indices: [.ml-meta, .ml-stuff], "
                    + "but in a future major version, direct access to system indices will be prevented by default"
            )
        );

    }

    public void testExternalSystemIndexAccess() {
        final ClusterState prev = systemIndexTestClusterState();
        ClusterState state = ClusterState.builder(prev)
            .metadata(
                Metadata.builder(prev.metadata()).put(indexBuilder(".external-sys-idx", Settings.EMPTY).state(State.OPEN).system(true))
            )
            .build();
        SystemIndices systemIndices = new SystemIndices(
            org.elasticsearch.core.List.of(
                new Feature(
                    "ml",
                    "ml indices",
                    org.elasticsearch.core.List.of(
                        new SystemIndexDescriptor(".ml-meta*", "ml meta"),
                        new SystemIndexDescriptor(".ml-stuff*", "other ml")
                    )
                ),
                new Feature(
                    "watcher",
                    "watcher indices",
                    org.elasticsearch.core.List.of(new SystemIndexDescriptor(".watches*", "watches index"))
                ),
                new Feature(
                    "stack-component",
                    "stack component",
                    org.elasticsearch.core.List.of(
                        new SystemIndexDescriptor(
                            ".external-sys-idx*",
                            "external",
                            Type.EXTERNAL_UNMANAGED,
                            org.elasticsearch.core.List.of("stack-component", "other")
                        )
                    )
                )
            )
        );
        indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext, systemIndices);

        {
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.FALSE.toString());
                SearchRequest request = new SearchRequest(".external-*");

                List<String> indexNames = resolveConcreteIndexNameList(state, request);
                assertThat(indexNames, contains(".external-sys-idx"));
                assertWarnings(
                    true,
                    new DeprecationWarning(
                        Level.WARN,
                        "this request accesses system indices: [.external-sys-idx], "
                            + "but in a future major version, direct access to system indices will be prevented by default"
                    )
                );
            }
        }
        {
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.FALSE.toString());
                SearchRequest request = new SearchRequest(".external-sys-idx");

                List<String> indexNames = resolveConcreteIndexNameList(state, request);
                assertThat(indexNames, contains(".external-sys-idx"));
                assertWarnings(
                    true,
                    new DeprecationWarning(
                        Level.WARN,
                        "this request accesses system indices: [.external-sys-idx], "
                            + "but in a future major version, direct access to system indices will be prevented by default"
                    )
                );
            }
        }
        // product origin = stack-component
        {
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.TRUE.toString());
                threadContext.putHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "stack-component");
                SearchRequest request = new SearchRequest(".external-*");

                List<String> indexNames = resolveConcreteIndexNameList(state, request);
                assertThat(indexNames, contains(".external-sys-idx"));
                assertWarnings();
            }
        }
        {
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.TRUE.toString());
                threadContext.putHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "stack-component");
                SearchRequest request = new SearchRequest(".external-sys-idx");

                List<String> indexNames = resolveConcreteIndexNameList(state, request);
                assertThat(indexNames, contains(".external-sys-idx"));
                assertWarnings();
            }
        }
        // product origin = other
        {
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.TRUE.toString());
                threadContext.putHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "other");
                SearchRequest request = new SearchRequest(".external-*");

                List<String> indexNames = resolveConcreteIndexNameList(state, request);
                assertThat(indexNames, contains(".external-sys-idx"));
                assertWarnings();
            }
        }
        {
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.TRUE.toString());
                threadContext.putHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "other");
                SearchRequest request = new SearchRequest(".external-sys-idx");

                List<String> indexNames = resolveConcreteIndexNameList(state, request);
                assertThat(indexNames, contains(".external-sys-idx"));
                assertWarnings();
            }
        }
    }

    public void testConcreteIndicesPreservesOrdering() {
        epochMillis = 1582761600L; // set to a date known to fail without #65027
        final String dataStreamName = "my-data-stream";
        IndexMetadata index1 = createBackingIndex(dataStreamName, 1, epochMillis).build();
        IndexMetadata index2 = createBackingIndex(dataStreamName, 2, epochMillis).build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(index1, false)
            .put(index2, false)
            .put(
                newInstance(
                    dataStreamName,
                    createTimestampField("@timestamp"),
                    org.elasticsearch.core.List.of(index1.getIndex(), index2.getIndex())
                )
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, dataStreamName);
            assertThat(result.length, equalTo(2));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, epochMillis)));
        }
    }

    public void testDataStreams() {
        final String dataStreamName = "my-data-stream";
        IndexMetadata index1 = createBackingIndex(dataStreamName, 1, epochMillis).build();
        IndexMetadata index2 = createBackingIndex(dataStreamName, 2, epochMillis).build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(index1, false)
            .put(index2, false)
            .put(
                newInstance(
                    dataStreamName,
                    createTimestampField("@timestamp"),
                    org.elasticsearch.core.List.of(index1.getIndex(), index2.getIndex())
                )
            );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-data-stream");
            assertThat(result.length, equalTo(2));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, epochMillis)));
        }
        {
            // Ignore data streams
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Exception e = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndices(state, indicesOptions, false, "my-data-stream")
            );
            assertThat(e.getMessage(), equalTo("no such index [my-data-stream]"));
        }
        {
            // Ignore data streams and allow no indices
            IndicesOptions indicesOptions = new IndicesOptions(
                EnumSet.of(IndicesOptions.Option.ALLOW_NO_INDICES),
                EnumSet.of(IndicesOptions.WildcardStates.OPEN)
            );
            Exception e = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndices(state, indicesOptions, false, "my-data-stream")
            );
            assertThat(e.getMessage(), equalTo("no such index [my-data-stream]"));
        }
        {
            // Ignore data streams, allow no indices and ignore unavailable
            IndicesOptions indicesOptions = new IndicesOptions(
                EnumSet.of(IndicesOptions.Option.ALLOW_NO_INDICES, IndicesOptions.Option.IGNORE_UNAVAILABLE),
                EnumSet.of(IndicesOptions.WildcardStates.OPEN)
            );
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, false, "my-data-stream");
            assertThat(result.length, equalTo(0));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index result = indexNameExpressionResolver.concreteWriteIndex(state, indicesOptions, "my-data-stream", false, true);
            assertThat(result.getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, epochMillis)));
        }
        {
            // Ignore data streams
            IndicesOptions indicesOptions = new IndicesOptions(
                EnumSet.noneOf(IndicesOptions.Option.class),
                EnumSet.of(IndicesOptions.WildcardStates.OPEN)
            );
            Exception e = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteWriteIndex(state, indicesOptions, "my-data-stream", true, false)
            );
            assertThat(e.getMessage(), equalTo("no such index [my-data-stream]"));
        }
        {
            // Ignore data streams and allow no indices
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Exception e = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteWriteIndex(state, indicesOptions, "my-data-stream", false, false)
            );
            assertThat(e.getMessage(), equalTo("no such index [my-data-stream]"));
        }
        {
            // Ignore data streams, allow no indices and ignore unavailable
            IndicesOptions indicesOptions = new IndicesOptions(
                EnumSet.of(IndicesOptions.Option.ALLOW_NO_INDICES, IndicesOptions.Option.IGNORE_UNAVAILABLE),
                EnumSet.of(IndicesOptions.WildcardStates.OPEN)
            );
            Exception e = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteWriteIndex(state, indicesOptions, "my-data-stream", false, false)
            );
            assertThat(e.getMessage(), equalTo("no such index [null]"));
        }
    }

    public void testDataStreamAliases() {
        String dataStream1 = "my-data-stream-1";
        IndexMetadata index1 = createBackingIndex(dataStream1, 1, epochMillis).build();
        IndexMetadata index2 = createBackingIndex(dataStream1, 2, epochMillis).build();
        String dataStream2 = "my-data-stream-2";
        IndexMetadata index3 = createBackingIndex(dataStream2, 1, epochMillis).build();
        IndexMetadata index4 = createBackingIndex(dataStream2, 2, epochMillis).build();
        String dataStream3 = "my-data-stream-3";
        IndexMetadata index5 = createBackingIndex(dataStream3, 1, epochMillis).build();
        IndexMetadata index6 = createBackingIndex(dataStream3, 2, epochMillis).build();

        String dataStreamAlias1 = "my-alias1";
        String dataStreamAlias2 = "my-alias2";
        String dataStreamAlias3 = "my-alias3";
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(index1, false)
            .put(index2, false)
            .put(index3, false)
            .put(index4, false)
            .put(index5, false)
            .put(index6, false)
            .put(newInstance(dataStream1, createTimestampField("@timestamp"), Arrays.asList(index1.getIndex(), index2.getIndex())))
            .put(newInstance(dataStream2, createTimestampField("@timestamp"), Arrays.asList(index3.getIndex(), index4.getIndex())))
            .put(newInstance(dataStream3, createTimestampField("@timestamp"), Arrays.asList(index5.getIndex(), index6.getIndex())));
        mdBuilder.put(dataStreamAlias1, dataStream1, null, null);
        mdBuilder.put(dataStreamAlias1, dataStream2, true, null);
        mdBuilder.put(dataStreamAlias2, dataStream2, null, null);
        mdBuilder.put(dataStreamAlias3, dataStream3, null, "{\"term\":{\"year\":2021}}");
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, dataStreamAlias1);
            assertThat(result, arrayContainingInAnyOrder(index1.getIndex(), index2.getIndex(), index3.getIndex(), index4.getIndex()));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, dataStreamAlias2);
            assertThat(result, arrayContainingInAnyOrder(index3.getIndex(), index4.getIndex()));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, dataStreamAlias3);
            assertThat(result, arrayContainingInAnyOrder(index5.getIndex(), index6.getIndex()));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Exception e = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndices(state, indicesOptions, false, dataStreamAlias1)
            );
            assertThat(e.getMessage(), equalTo("no such index [" + dataStreamAlias1 + "]"));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Exception e = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndices(state, indicesOptions, false, dataStreamAlias2)
            );
            assertThat(e.getMessage(), equalTo("no such index [" + dataStreamAlias2 + "]"));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Exception e = expectThrows(
                IndexNotFoundException.class,
                () -> indexNameExpressionResolver.concreteIndices(state, indicesOptions, false, dataStreamAlias3)
            );
            assertThat(e.getMessage(), equalTo("no such index [" + dataStreamAlias3 + "]"));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-alias*");
            assertThat(
                result,
                arrayContainingInAnyOrder(
                    index1.getIndex(),
                    index2.getIndex(),
                    index3.getIndex(),
                    index4.getIndex(),
                    index5.getIndex(),
                    index6.getIndex()
                )
            );
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, false, "my-alias*");
            assertThat(result, arrayWithSize(0));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index result = indexNameExpressionResolver.concreteWriteIndex(state, indicesOptions, dataStreamAlias1, false, true);
            assertThat(result, notNullValue());
            assertThat(result.getName(), backingIndexEqualTo(dataStream2, 2));
        }
    }

    public void testDataStreamsWithWildcardExpression() {
        final String dataStream1 = "logs-mysql";
        final String dataStream2 = "logs-redis";
        IndexMetadata index1 = createBackingIndex(dataStream1, 1, epochMillis).build();
        IndexMetadata index2 = createBackingIndex(dataStream1, 2, epochMillis).build();
        IndexMetadata index3 = createBackingIndex(dataStream2, 1, epochMillis).build();
        IndexMetadata index4 = createBackingIndex(dataStream2, 2, epochMillis).build();
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(index1, false)
            .put(index2, false)
            .put(index3, false)
            .put(index4, false)
            .put(
                newInstance(
                    dataStream1,
                    createTimestampField("@timestamp"),
                    org.elasticsearch.core.List.of(index1.getIndex(), index2.getIndex())
                )
            )
            .put(
                newInstance(
                    dataStream2,
                    createTimestampField("@timestamp"),
                    org.elasticsearch.core.List.of(index3.getIndex(), index4.getIndex())
                )
            );

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "logs-*");
            Arrays.sort(result, Comparator.comparing(Index::getName));
            assertThat(result.length, equalTo(4));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 2, epochMillis)));
            assertThat(result[2].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream2, 1, epochMillis)));
            assertThat(result[3].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream2, 2, epochMillis)));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(
                state,
                indicesOptions,
                true,
                randomFrom(new String[] { "*" }, new String[] { "_all" }, new String[0])
            );
            Arrays.sort(result, Comparator.comparing(Index::getName));
            assertThat(result.length, equalTo(4));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 2, epochMillis)));
            assertThat(result[2].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream2, 1, epochMillis)));
            assertThat(result[3].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream2, 2, epochMillis)));
            ;
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "logs-m*");
            Arrays.sort(result, Comparator.comparing(Index::getName));
            assertThat(result.length, equalTo(2));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 2, epochMillis)));
        }
        {
            IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN; // without include data streams
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, "logs-*");
            assertThat(result.length, equalTo(0));
        }
    }

    public void testDataStreamsWithClosedBackingIndicesAndWildcardExpressions() {
        final String dataStream1 = "logs-mysql";
        final String dataStream2 = "logs-redis";
        IndexMetadata index1 = createBackingIndex(dataStream1, 1, epochMillis).state(State.CLOSE).build();
        IndexMetadata index2 = createBackingIndex(dataStream1, 2, epochMillis).build();
        IndexMetadata index3 = createBackingIndex(dataStream2, 1, epochMillis).state(State.CLOSE).build();
        IndexMetadata index4 = createBackingIndex(dataStream2, 2, epochMillis).build();
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(index1, false)
            .put(index2, false)
            .put(index3, false)
            .put(index4, false)
            .put(
                newInstance(
                    dataStream1,
                    createTimestampField("@timestamp"),
                    org.elasticsearch.core.List.of(index1.getIndex(), index2.getIndex())
                )
            )
            .put(
                newInstance(
                    dataStream2,
                    createTimestampField("@timestamp"),
                    org.elasticsearch.core.List.of(index3.getIndex(), index4.getIndex())
                )
            );

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "logs-*");
            Arrays.sort(result, Comparator.comparing(Index::getName));
            assertThat(result.length, equalTo(2));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 2, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream2, 2, epochMillis)));
        }
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "*");
            Arrays.sort(result, Comparator.comparing(Index::getName));
            assertThat(result.length, equalTo(2));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 2, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream2, 2, epochMillis)));
        }
    }

    public void testDataStreamsWithRegularIndexAndAlias() {
        final String dataStream1 = "logs-foobar";
        IndexMetadata index1 = createBackingIndex(dataStream1, 1, epochMillis).build();
        IndexMetadata index2 = createBackingIndex(dataStream1, 2, epochMillis).build();
        IndexMetadata justAnIndex = IndexMetadata.builder("logs-foobarbaz-0")
            .settings(ESTestCase.settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putAlias(new AliasMetadata.Builder("logs-foobarbaz"))
            .build();

        ClusterState state = ClusterState.builder(new ClusterName("_name"))
            .metadata(
                Metadata.builder()
                    .put(index1, false)
                    .put(index2, false)
                    .put(justAnIndex, false)
                    .put(newInstance(dataStream1, createTimestampField("@timestamp"), Arrays.asList(index1.getIndex(), index2.getIndex())))
            )
            .build();

        IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();
        Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "logs-*");
        Arrays.sort(result, Comparator.comparing(Index::getName));
        assertThat(result.length, equalTo(3));
        assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 1, epochMillis)));
        assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStream1, 2, epochMillis)));
        assertThat(result[2].getName(), equalTo("logs-foobarbaz-0"));
    }

    public void testHiddenDataStreams() {
        final String dataStream1 = "logs-foobar";
        IndexMetadata index1 = createBackingIndex(dataStream1, 1, epochMillis).build();
        IndexMetadata index2 = createBackingIndex(dataStream1, 2, epochMillis).build();
        IndexMetadata justAnIndex = IndexMetadata.builder("logs-foobarbaz-0")
            .settings(ESTestCase.settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState state = ClusterState.builder(new ClusterName("_name"))
            .metadata(
                Metadata.builder()
                    .put(index1, false)
                    .put(index2, false)
                    .put(justAnIndex, false)
                    .put(
                        new DataStream(
                            dataStream1,
                            createTimestampField("@timestamp"),
                            Arrays.asList(index1.getIndex(), index2.getIndex()),
                            2,
                            Collections.emptyMap(),
                            true,
                            false,
                            false
                        )
                    )
            )
            .build();

        Index[] result = indexNameExpressionResolver.concreteIndices(state, IndicesOptions.strictExpandHidden(), true, "logs-*");
        assertThat(result, arrayContainingInAnyOrder(index1.getIndex(), index2.getIndex(), justAnIndex.getIndex()));

        result = indexNameExpressionResolver.concreteIndices(state, IndicesOptions.strictExpandOpen(), true, "logs-*");
        assertThat(result, arrayContaining(justAnIndex.getIndex()));
    }

    public void testDataStreamsNames() {
        final String dataStream1 = "logs-foobar";
        final String dataStream2 = "other-foobar";
        IndexMetadata index1 = createBackingIndex(dataStream1, 1).build();
        IndexMetadata index2 = createBackingIndex(dataStream1, 2).build();
        IndexMetadata justAnIndex = IndexMetadata.builder("logs-foobarbaz-0")
            .settings(ESTestCase.settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putAlias(new AliasMetadata.Builder("logs-foobarbaz"))
            .build();

        IndexMetadata index3 = createBackingIndex(dataStream2, 1).build();
        IndexMetadata index4 = createBackingIndex(dataStream2, 2).build();

        ClusterState state = ClusterState.builder(new ClusterName("_name"))
            .metadata(
                Metadata.builder()
                    .put(index1, false)
                    .put(index2, false)
                    .put(index3, false)
                    .put(index4, false)
                    .put(justAnIndex, false)
                    .put(newInstance(dataStream1, createTimestampField("@timestamp"), Arrays.asList(index1.getIndex(), index2.getIndex())))
                    .put(newInstance(dataStream2, createTimestampField("@timestamp"), Arrays.asList(index3.getIndex(), index4.getIndex())))
            )
            .build();

        List<String> names = indexNameExpressionResolver.dataStreamNames(state, IndicesOptions.lenientExpand(), "log*");
        assertEquals(Collections.singletonList(dataStream1), names);

        names = indexNameExpressionResolver.dataStreamNames(state, IndicesOptions.lenientExpand(), dataStream1);
        assertEquals(Collections.singletonList(dataStream1), names);

        names = indexNameExpressionResolver.dataStreamNames(state, IndicesOptions.lenientExpand(), "other*");
        assertEquals(Collections.singletonList(dataStream2), names);

        names = indexNameExpressionResolver.dataStreamNames(state, IndicesOptions.lenientExpand(), "*foobar");
        assertThat(names, containsInAnyOrder(dataStream1, dataStream2));

        names = indexNameExpressionResolver.dataStreamNames(state, IndicesOptions.lenientExpand(), "notmatched");
        assertThat(names, empty());

        names = indexNameExpressionResolver.dataStreamNames(state, IndicesOptions.lenientExpand(), index3.getIndex().getName());
        assertThat(names, empty());

        names = indexNameExpressionResolver.dataStreamNames(state, IndicesOptions.lenientExpand(), "*", "-logs-foobar");
        assertThat(names, containsInAnyOrder(dataStream2));

        names = indexNameExpressionResolver.dataStreamNames(state, IndicesOptions.lenientExpand(), "*", "-*");
        assertThat(names, empty());
    }

    public void testMathExpressionSupport() {
        Instant instant = LocalDate.of(2021, 01, 11).atStartOfDay().toInstant(ZoneOffset.UTC);
        String resolved = this.indexNameExpressionResolver.resolveDateMathExpression("<a-name-{now/M{yyyy-MM}}>", instant.toEpochMilli());

        assertEquals(resolved, "a-name-2021-01");
    }

    public void testMathExpressionSupportWithOlderDate() {

        Instant instant = LocalDate.of(2020, 12, 2).atStartOfDay().toInstant(ZoneOffset.UTC);
        final String indexName = "<older-date-{now/M{yyyy-MM}}>";
        String resolved = this.indexNameExpressionResolver.resolveDateMathExpression(indexName, instant.toEpochMilli());

        assertEquals(resolved, "older-date-2020-12");
    }

    private ClusterState systemIndexTestClusterState() {
        Settings settings = Settings.builder().build();
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder(".ml-meta", settings).state(State.OPEN).system(true))
            .put(indexBuilder(".watches", settings).state(State.OPEN).system(true))
            .put(indexBuilder(".ml-stuff", settings).state(State.OPEN).system(true))
            .put(indexBuilder("some-other-index").state(State.OPEN));
        SystemIndices systemIndices = new SystemIndices(
            org.elasticsearch.core.List.of(
                new Feature(
                    "ml",
                    "ml indices",
                    org.elasticsearch.core.List.of(
                        new SystemIndexDescriptor(".ml-meta*", "ml meta"),
                        new SystemIndexDescriptor(".ml-stuff*", "other ml")
                    )
                ),
                new Feature(
                    "watcher",
                    "watcher indices",
                    org.elasticsearch.core.List.of(new SystemIndexDescriptor(".watches*", "watches index"))
                )
            )
        );
        indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext, systemIndices);
        return ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
    }

    private List<String> resolveConcreteIndexNameList(ClusterState state, SearchRequest request) {
        return Arrays.stream(indexNameExpressionResolver.concreteIndices(state, request)).map(Index::getName).collect(Collectors.toList());
    }
}
