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

import com.google.common.collect.Sets;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetaData.State;
import org.elasticsearch.common.Strings;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.HashSet;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.*;

/**
 */
public class MetaDataTests extends ElasticsearchTestCase {

    @Test
    public void testIndexOptions_strict() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        MetaData md = mdBuilder.build();

        IndicesOptions[] indicesOptions = new IndicesOptions[]{ IndicesOptions.strictExpandOpen(), IndicesOptions.strictExpand()};

        for (IndicesOptions options : indicesOptions) {
            String[] results = md.concreteIndices(options, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            try {
                md.concreteIndices(options, "bar");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("bar"));
            }

            results = md.concreteIndices(options, "foofoo", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = md.concreteIndices(options, "foofoobar");
            assertEquals(2, results.length);
            assertEquals("foo", results[0]);
            assertEquals("foobar", results[1]);

            try {
                md.concreteIndices(options, "bar");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("bar"));
            }

            try {
                md.concreteIndices(options, "foo", "bar");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("bar"));
            }

            results = md.concreteIndices(options, "barbaz", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            try {
                md.concreteIndices(options, "barbaz", "bar");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("bar"));
            }

            results = md.concreteIndices(options, "baz*");
            assertThat(results, emptyArray());

            results = md.concreteIndices(options, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);
        }

        String[] results = md.concreteIndices(IndicesOptions.strictExpandOpen(), Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        results = md.concreteIndices(IndicesOptions.strictExpandOpen(), null);
        assertEquals(3, results.length);

        results = md.concreteIndices(IndicesOptions.strictExpand(), Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);

        results = md.concreteIndices(IndicesOptions.strictExpand(), null);
        assertEquals(4, results.length);

        results = md.concreteIndices(IndicesOptions.strictExpandOpen(), "foofoo*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo"));

        results = md.concreteIndices(IndicesOptions.strictExpand(), "foofoo*");
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
        MetaData md = mdBuilder.build();

        IndicesOptions lenientExpand = IndicesOptions.fromOptions(true, true, true, true);
        IndicesOptions[] indicesOptions = new IndicesOptions[]{ IndicesOptions.lenientExpandOpen(), lenientExpand};

        for (IndicesOptions options : indicesOptions) {
            String[] results = md.concreteIndices(options, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = md.concreteIndices(options, "bar");
            assertThat(results, emptyArray());

            results = md.concreteIndices(options, "foofoo", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = md.concreteIndices(options, "foofoobar");
            assertEquals(2, results.length);
            assertEquals("foo", results[0]);
            assertEquals("foobar", results[1]);

            results = md.concreteIndices(options, "foo", "bar");
            assertEquals(1, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo"));

            results = md.concreteIndices(options, "barbaz", "foobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

            results = md.concreteIndices(options, "barbaz", "bar");
            assertEquals(1, results.length);
            assertThat(results, arrayContainingInAnyOrder("foofoo"));

            results = md.concreteIndices(options, "baz*");
            assertThat(results, emptyArray());

            results = md.concreteIndices(options, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);
        }

        String[] results = md.concreteIndices(IndicesOptions.lenientExpandOpen(), Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        results = md.concreteIndices(lenientExpand, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);

        results = md.concreteIndices(IndicesOptions.lenientExpandOpen(), "foofoo*");
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foobar", "foofoo"));

        results = md.concreteIndices(lenientExpand, "foofoo*");
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
        MetaData md = mdBuilder.build();

        IndicesOptions expandOpen = IndicesOptions.fromOptions(true, false, true, false);
        IndicesOptions expand = IndicesOptions.fromOptions(true, false, true, true);
        IndicesOptions[] indicesOptions = new IndicesOptions[]{expandOpen, expand};

        for (IndicesOptions options : indicesOptions) {
            String[] results = md.concreteIndices(options, "foo");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            try {
                md.concreteIndices(options, "bar");
                fail();
            } catch(IndexMissingException e) {
                assertThat(e.index().name(), equalTo("bar"));
            }

            try {
                md.concreteIndices(options, "baz*");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("baz*"));
            }

            try {
                md.concreteIndices(options, "foo", "baz*");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("baz*"));
            }
        }

        String[] results = md.concreteIndices(expandOpen, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);

        results = md.concreteIndices(expand, Strings.EMPTY_ARRAY);
        assertEquals(4, results.length);
    }

    @Test
    public void testIndexOptions_wildcardExpansion() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("bar"))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("barbaz")));
        MetaData md = mdBuilder.build();

        // Only closed
        IndicesOptions options = IndicesOptions.fromOptions(false, true, false, true);
        String[] results = md.concreteIndices(options, Strings.EMPTY_ARRAY);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        results = md.concreteIndices(options, "foo*");
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        // no wildcards, so wildcard expansion don't apply
        results = md.concreteIndices(options, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        // Only open
        options = IndicesOptions.fromOptions(false, true, true, false);
        results = md.concreteIndices(options, Strings.EMPTY_ARRAY);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar"));

        results = md.concreteIndices(options, "foo*");
        assertEquals(1, results.length);
        assertEquals("foobar", results[0]);

        results = md.concreteIndices(options, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        // Open and closed
        options = IndicesOptions.fromOptions(false, true, true, true);
        results = md.concreteIndices(options, Strings.EMPTY_ARRAY);
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar", "foo"));

        results = md.concreteIndices(options, "foo*");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foobar", "foo"));

        results = md.concreteIndices(options, "bar");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = md.concreteIndices(options, "-foo*");
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = md.concreteIndices(options, "-*");
        assertEquals(0, results.length);

        options = IndicesOptions.fromOptions(false, false, true, true);
        try {
            md.concreteIndices(options, "-*");
            fail();
        } catch (IndexMissingException e) {
            assertThat(e.index().name(), equalTo("[-*]"));
        }
    }

    @Test
    public void testIndexOptions_noExpandWildcards() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        MetaData md = mdBuilder.build();

        //ignore unavailable and allow no indices
        {
            IndicesOptions noExpandLenient = IndicesOptions.fromOptions(true, true, false, false);

            String[] results = md.concreteIndices(noExpandLenient, "baz*");
            assertThat(results, emptyArray());

            results = md.concreteIndices(noExpandLenient, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = md.concreteIndices(noExpandLenient, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));

            results = md.concreteIndices(noExpandLenient, null);
            assertEquals(0, results.length);

            results = md.concreteIndices(noExpandLenient, Strings.EMPTY_ARRAY);
            assertEquals(0, results.length);
        }

        //ignore unavailable but don't allow no indices
        {
            IndicesOptions noExpandDisallowEmpty = IndicesOptions.fromOptions(true, false, false, false);

            try {
                md.concreteIndices(noExpandDisallowEmpty, "baz*");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("baz*"));
            }

            String[] results = md.concreteIndices(noExpandDisallowEmpty, "foo", "baz*");
            assertEquals(1, results.length);
            assertEquals("foo", results[0]);

            results = md.concreteIndices(noExpandDisallowEmpty, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
        }

        //error on unavailable but allow no indices
        {
            IndicesOptions noExpandErrorUnavailable = IndicesOptions.fromOptions(false, true, false, false);

            String[] results = md.concreteIndices(noExpandErrorUnavailable, "baz*");
            assertThat(results, emptyArray());

            try {
                md.concreteIndices(noExpandErrorUnavailable, "foo", "baz*");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("baz*"));
            }

            results = md.concreteIndices(noExpandErrorUnavailable, "foofoobar");
            assertEquals(2, results.length);
            assertThat(results, arrayContainingInAnyOrder("foo", "foobar"));
        }

        //error on both unavailable and no indices
        {
            IndicesOptions noExpandStrict = IndicesOptions.fromOptions(false, false, false, false);

            try {
                md.concreteIndices(noExpandStrict, "baz*");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("baz*"));
            }

            try {
                md.concreteIndices(noExpandStrict, "foo", "baz*");
                fail();
            } catch (IndexMissingException e) {
                assertThat(e.index().name(), equalTo("baz*"));
            }

            String[] results = md.concreteIndices(noExpandStrict, "foofoobar");
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
        MetaData md = mdBuilder.build();

        //error on both unavailable and no indices + every alias needs to expand to a single index

        try {
            md.concreteIndices(IndicesOptions.strictSingleIndexNoExpandForbidClosed(), "baz*");
            fail();
        } catch (IndexMissingException e) {
            assertThat(e.index().name(), equalTo("baz*"));
        }

        try {
            md.concreteIndices(IndicesOptions.strictSingleIndexNoExpandForbidClosed(), "foo", "baz*");
            fail();
        } catch (IndexMissingException e) {
            assertThat(e.index().name(), equalTo("baz*"));
        }

        try {
            md.concreteIndices(IndicesOptions.strictSingleIndexNoExpandForbidClosed(), "foofoobar");
            fail();
        } catch(ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Alias [foofoobar] has more than one indices associated with it"));
        }

        try {
            md.concreteIndices(IndicesOptions.strictSingleIndexNoExpandForbidClosed(), "foo", "foofoobar");
            fail();
        } catch(ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Alias [foofoobar] has more than one indices associated with it"));
        }

        try {
            md.concreteIndices(IndicesOptions.strictSingleIndexNoExpandForbidClosed(), "foofoo-closed", "foofoobar");
            fail();
        } catch(IndexClosedException e) {
            assertThat(e.getMessage(), equalTo("closed"));
            assertEquals(e.index().getName(), "foofoo-closed");
        }

        String[] results = md.concreteIndices(IndicesOptions.strictSingleIndexNoExpandForbidClosed(), "foo", "barbaz");
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo", "foofoo"));
    }

    @Test
    public void testIndexOptions_emptyCluster() {
        MetaData md = MetaData.builder().build();
        IndicesOptions options = IndicesOptions.strictExpandOpen();

        String[] results = md.concreteIndices(options, Strings.EMPTY_ARRAY);
        assertThat(results, emptyArray());
        try {
            md.concreteIndices(options, "foo");
            fail();
        } catch (IndexMissingException e) {
            assertThat(e.index().name(), equalTo("foo"));
        }
        results = md.concreteIndices(options, "foo*");
        assertThat(results, emptyArray());
        try {
            md.concreteIndices(options, "foo*", "bar");
            fail();
        } catch (IndexMissingException e) {
            assertThat(e.index().name(), equalTo("bar"));
        }


        options = IndicesOptions.lenientExpandOpen();
        results = md.concreteIndices(options, Strings.EMPTY_ARRAY);
        assertThat(results, emptyArray());
        results = md.concreteIndices(options, "foo");
        assertThat(results, emptyArray());
        results = md.concreteIndices(options, "foo*");
        assertThat(results, emptyArray());
        results = md.concreteIndices(options, "foo*", "bar");
        assertThat(results, emptyArray());

        options = IndicesOptions.fromOptions(true, false, true, false);
        try {
            md.concreteIndices(options, Strings.EMPTY_ARRAY);
        } catch (IndexMissingException e) {
            assertThat(e.index().name(), equalTo("_all"));
        }
    }

    @Test
    public void testConvertWildcardsJustIndicesTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("testXYY"))
                .put(indexBuilder("testYYY"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testXXX"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testXXX", "testYYY"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testXXX", "ku*"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX", "kuku")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"test*"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testX*"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testX*", "kuku"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX", "testXYY", "kuku")));
    }

    @Test
    public void testConvertWildcardsTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX").putAlias(AliasMetaData.builder("alias1")).putAlias(AliasMetaData.builder("alias2")))
                .put(indexBuilder("testXYY").putAlias(AliasMetaData.builder("alias2")))
                .put(indexBuilder("testYYY").putAlias(AliasMetaData.builder("alias3")))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testYY*", "alias*"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("alias1", "alias2", "alias3", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"-kuku"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"+test*", "-testYYY"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"+testX*", "+testYYY"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"+testYYY", "+testX*"}, IndicesOptions.lenientExpandOpen())), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
    }

    @Test
    public void testConvertWildcardsOpenClosedIndicesTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX").state(State.OPEN))
                .put(indexBuilder("testXXY").state(State.OPEN))
                .put(indexBuilder("testXYY").state(State.CLOSE))
                .put(indexBuilder("testYYY").state(State.OPEN))
                .put(indexBuilder("testYYX").state(State.CLOSE))
                .put(indexBuilder("kuku").state(State.OPEN));
        MetaData md = mdBuilder.build();
        // Can't test when wildcard expansion is turned off here as convertFromWildcards shouldn't be called in this case.  Tests for this are covered in the concreteIndices() tests
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testX*"}, IndicesOptions.fromOptions(true, true, true, true))), equalTo(newHashSet("testXXX", "testXXY", "testXYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testX*"}, IndicesOptions.fromOptions(true, true, false, true))), equalTo(newHashSet("testXYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testX*"}, IndicesOptions.fromOptions(true, true, true, false))), equalTo(newHashSet("testXXX", "testXXY")));
    }

    private IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(settings(Version.CURRENT).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }

    @Test(expected = IndexMissingException.class)
    public void testConcreteIndicesIgnoreIndicesOneMissingIndex() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        md.concreteIndices(IndicesOptions.strictExpandOpen(), "testZZZ");
    }

    @Test
    public void testConcreteIndicesIgnoreIndicesOneMissingIndexOtherFound() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(IndicesOptions.lenientExpandOpen(), "testXXX", "testZZZ")), equalTo(newHashSet("testXXX")));
    }

    @Test(expected = IndexMissingException.class)
    public void testConcreteIndicesIgnoreIndicesAllMissing() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(IndicesOptions.strictExpandOpen(), "testMo", "testMahdy")), equalTo(newHashSet("testXXX")));
    }

    @Test
    public void testConcreteIndicesIgnoreIndicesEmptyRequest() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(IndicesOptions.lenientExpandOpen(), new String[]{})), equalTo(Sets.newHashSet("kuku", "testXXX")));
    }

    @Test
    public void testConcreteIndicesWildcardExpansion() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX").state(State.OPEN))
                .put(indexBuilder("testXXY").state(State.OPEN))
                .put(indexBuilder("testXYY").state(State.CLOSE))
                .put(indexBuilder("testYYY").state(State.OPEN))
                .put(indexBuilder("testYYX").state(State.OPEN));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(IndicesOptions.fromOptions(true, true, false, false), "testX*")), equalTo(new HashSet<String>()));
        assertThat(newHashSet(md.concreteIndices(IndicesOptions.fromOptions(true, true, true, false), "testX*")), equalTo(newHashSet("testXXX", "testXXY")));
        assertThat(newHashSet(md.concreteIndices(IndicesOptions.fromOptions(true, true, false, true), "testX*")), equalTo(newHashSet("testXYY")));
        assertThat(newHashSet(md.concreteIndices(IndicesOptions.fromOptions(true, true, true, true), "testX*")), equalTo(newHashSet("testXXX", "testXXY", "testXYY")));
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
            MetaData metadata = MetaData.builder().build();

            // with no indices, asking for all indices should return empty list or exception, depending on indices options
            if (indicesOptions.allowNoIndices()) {
                String[] concreteIndices = metadata.concreteIndices(indicesOptions, allIndices);
                assertThat(concreteIndices, notNullValue());
                assertThat(concreteIndices.length, equalTo(0));
            } else {
                checkCorrectException(metadata, indicesOptions, allIndices);
            }

            // with existing indices, asking for all indices should return all open/closed indices depending on options
            metadata = MetaData.builder()
                    .put(indexBuilder("aaa").state(State.OPEN).putAlias(AliasMetaData.builder("aaa_alias1")))
                    .put(indexBuilder("bbb").state(State.OPEN).putAlias(AliasMetaData.builder("bbb_alias1")))
                    .put(indexBuilder("ccc").state(State.CLOSE).putAlias(AliasMetaData.builder("ccc_alias1")))
                    .build();
            if (indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed() || indicesOptions.allowNoIndices()) {
                String[] concreteIndices = metadata.concreteIndices(indicesOptions, allIndices);
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
                checkCorrectException(metadata, indicesOptions, allIndices);
            }
        }
    }

    /**
     * check for correct exception type depending on indicesOptions and provided index name list
     */
    private void checkCorrectException(MetaData metadata, IndicesOptions indicesOptions, String[] allIndices) {
     // two different exception types possible
        if (!(indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed())
                && (allIndices == null || allIndices.length == 0)) {
            try {
                metadata.concreteIndices(indicesOptions, allIndices);
                fail("no wildcard expansion and null or empty list argument should trigger ElasticsearchIllegalArgumentException");
            } catch (ElasticsearchIllegalArgumentException e) {
                // expected
            }
        } else {
            try {
                metadata.concreteIndices(indicesOptions, allIndices);
                fail("wildcard expansion on should trigger IndexMissingException");
            } catch (IndexMissingException e) {
                // expected
            }
        }
    }

    /**
     * test resolving wildcard pattern that matches no index of alias for random IndicesOptions
     */
    @Test
    public void testConcreteIndicesWildcardNoMatch() {
        for (int i = 0; i < 10; i++) {
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
            MetaData metadata = MetaData.builder().build();

            metadata = MetaData.builder()
                    .put(indexBuilder("aaa").state(State.OPEN).putAlias(AliasMetaData.builder("aaa_alias1")))
                    .put(indexBuilder("bbb").state(State.OPEN).putAlias(AliasMetaData.builder("bbb_alias1")))
                    .put(indexBuilder("ccc").state(State.CLOSE).putAlias(AliasMetaData.builder("ccc_alias1")))
                    .build();

            // asking for non existing wildcard pattern should return empty list or exception
            if (indicesOptions.allowNoIndices()) {
                String[] concreteIndices = metadata.concreteIndices(indicesOptions, "Foo*");
                assertThat(concreteIndices, notNullValue());
                assertThat(concreteIndices.length, equalTo(0));
            } else {
                try {
                    metadata.concreteIndices(indicesOptions, "Foo*");
                    fail("expecting exeption when result empty and allowNoIndicec=false");
                } catch (IndexMissingException e) {
                    // expected exception
                }
            }
        }
    }

    @Test
    public void testIsAllIndices_null() throws Exception {
        assertThat(MetaData.isAllIndices(null), equalTo(true));
    }

    @Test
    public void testIsAllIndices_empty() throws Exception {
        assertThat(MetaData.isAllIndices(new String[0]), equalTo(true));
    }

    @Test
    public void testIsAllIndices_explicitAll() throws Exception {
        assertThat(MetaData.isAllIndices(new String[]{"_all"}), equalTo(true));
    }

    @Test
    public void testIsAllIndices_explicitAllPlusOther() throws Exception {
        assertThat(MetaData.isAllIndices(new String[]{"_all", "other"}), equalTo(false));
    }

    @Test
    public void testIsAllIndices_normalIndexes() throws Exception {
        assertThat(MetaData.isAllIndices(new String[]{"index1", "index2", "index3"}), equalTo(false));
    }

    @Test
    public void testIsAllIndices_wildcard() throws Exception {
        assertThat(MetaData.isAllIndices(new String[]{"*"}), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_null() throws Exception {
        assertThat(MetaData.isExplicitAllPattern(null), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_empty() throws Exception {
        assertThat(MetaData.isExplicitAllPattern(new String[0]), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_explicitAll() throws Exception {
        assertThat(MetaData.isExplicitAllPattern(new String[]{"_all"}), equalTo(true));
    }

    @Test
    public void testIsExplicitAllIndices_explicitAllPlusOther() throws Exception {
        assertThat(MetaData.isExplicitAllPattern(new String[]{"_all", "other"}), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_normalIndexes() throws Exception {
        assertThat(MetaData.isExplicitAllPattern(new String[]{"index1", "index2", "index3"}), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_wildcard() throws Exception {
        assertThat(MetaData.isExplicitAllPattern(new String[]{"*"}), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_explicitList() throws Exception {
        //even though it does identify all indices, it's not a pattern but just an explicit list of them
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(concreteIndices, concreteIndices), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_onlyWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(indicesOrAliases, concreteIndices), equalTo(true));
    }

    @Test
    public void testIsPatternMatchingAllIndices_matchingTrailingWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"index*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(indicesOrAliases, concreteIndices), equalTo(true));
    }

    @Test
    public void testIsPatternMatchingAllIndices_nonMatchingTrailingWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"index*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        String[] allConcreteIndices = new String[]{"index1", "index2", "index3", "a", "b"};
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(indicesOrAliases, concreteIndices), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_matchingSingleExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"-index1", "+index1"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(indicesOrAliases, concreteIndices), equalTo(true));
    }

    @Test
    public void testIsPatternMatchingAllIndices_nonMatchingSingleExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"-index1"};
        String[] concreteIndices = new String[]{"index2", "index3"};
        String[] allConcreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(indicesOrAliases, concreteIndices), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_matchingTrailingWildcardAndExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"index*", "-index1", "+index1"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(concreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(indicesOrAliases, concreteIndices), equalTo(true));
    }

    @Test
    public void testIsPatternMatchingAllIndices_nonMatchingTrailingWildcardAndExclusion() throws Exception {
        String[] indicesOrAliases = new String[]{"index*", "-index1"};
        String[] concreteIndices = new String[]{"index2", "index3"};
        String[] allConcreteIndices = new String[]{"index1", "index2", "index3"};
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(indicesOrAliases, concreteIndices), equalTo(false));
    }

    @Test
    public void testIndexOptions_failClosedIndicesAndAliases() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo1-closed").state(IndexMetaData.State.CLOSE).putAlias(AliasMetaData.builder("foobar1-closed")).putAlias(AliasMetaData.builder("foobar2-closed")))
                .put(indexBuilder("foo2-closed").state(IndexMetaData.State.CLOSE).putAlias(AliasMetaData.builder("foobar2-closed")))
                .put(indexBuilder("foo3").putAlias(AliasMetaData.builder("foobar2-closed")));
        MetaData md = mdBuilder.build();

        IndicesOptions options = IndicesOptions.strictExpandOpenAndForbidClosed();
        try {
            md.concreteIndices(options, "foo1-closed");
            fail("foo1-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        try {
            md.concreteIndices(options, "foobar1-closed");
            fail("foo1-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        options = IndicesOptions.fromOptions(true, options.allowNoIndices(), options.expandWildcardsOpen(), options.expandWildcardsClosed(), options);
        String[] results = md.concreteIndices(options, "foo1-closed");
        assertThat(results, emptyArray());

        results = md.concreteIndices(options, "foobar1-closed");
        assertThat(results, emptyArray());

        options = IndicesOptions.lenientExpandOpen();
        results = md.concreteIndices(options, "foo1-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo1-closed"));

        results = md.concreteIndices(options, "foobar1-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo1-closed"));

        // testing an alias pointing to three indices:
        options = IndicesOptions.strictExpandOpenAndForbidClosed();
        try {
            md.concreteIndices(options, "foobar2-closed");
            fail("foo2-closed should be closed, but it is open");
        } catch (IndexClosedException e) {
            // expected
        }

        options = IndicesOptions.fromOptions(true, options.allowNoIndices(), options.expandWildcardsOpen(), options.expandWildcardsClosed(), options);
        results = md.concreteIndices(options, "foobar2-closed");
        assertThat(results, arrayWithSize(1));
        assertThat(results, arrayContaining("foo3"));

        options = IndicesOptions.lenientExpandOpen();
        results = md.concreteIndices(options, "foobar2-closed");
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
