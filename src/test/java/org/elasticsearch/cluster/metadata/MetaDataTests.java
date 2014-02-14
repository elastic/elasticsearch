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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class MetaDataTests extends ElasticsearchTestCase {

    @Test
    public void testIndexOptions_strict() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo"))
                .put(indexBuilder("foobar"))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        MetaData md = mdBuilder.build();

        IndicesOptions options = IndicesOptions.strict();

        String[] results = md.concreteIndices(Strings.EMPTY_ARRAY, options);
        assertEquals(3, results.length);

        results = md.concreteIndices(new String[]{"foo"}, options);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        try {
            md.concreteIndices(new String[]{"bar"}, options);
            fail();
        } catch (IndexMissingException e) {}

        results = md.concreteIndices(new String[]{"foofoo", "foobar"}, options);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

        try {
            md.concreteIndices(new String[]{"foo", "bar"}, options);
            fail();
        } catch (IndexMissingException e) {}

        results = md.concreteIndices(new String[]{"barbaz", "foobar"}, options);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

        try {
            md.concreteIndices(new String[]{"barbaz", "bar"}, options);
            fail();
        } catch (IndexMissingException e) {}

        results = md.concreteIndices(new String[]{"baz*"}, options);
        assertThat(results, emptyArray());

        results = md.concreteIndices(new String[]{"foo", "baz*"}, options);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);
    }

    @Test
    public void testIndexOptions_lenient() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo"))
                .put(indexBuilder("foobar"))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        MetaData md = mdBuilder.build();

        IndicesOptions options = IndicesOptions.lenient();

        String[] results = md.concreteIndices(Strings.EMPTY_ARRAY, options);
        assertEquals(3, results.length);

        results = md.concreteIndices(new String[]{"foo"}, options);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        results = md.concreteIndices(new String[]{"bar"}, options);
        assertThat(results, emptyArray());

        results = md.concreteIndices(new String[]{"foofoo", "foobar"}, options);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

        results = md.concreteIndices(new String[]{"foo", "bar"}, options);
        assertEquals(1, results.length);
        assertThat(results, arrayContainingInAnyOrder("foo"));

        results = md.concreteIndices(new String[]{"barbaz", "foobar"}, options);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foofoo", "foobar"));

        results = md.concreteIndices(new String[]{"barbaz", "bar"}, options);
        assertEquals(1, results.length);
        assertThat(results, arrayContainingInAnyOrder("foofoo"));

        results = md.concreteIndices(new String[]{"baz*"}, options);
        assertThat(results, emptyArray());

        results = md.concreteIndices(new String[]{"foo", "baz*"}, options);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);
    }

    @Test
    public void testIndexOptions_allowUnavailableExpandOpenDisAllowEmpty() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo"))
                .put(indexBuilder("foobar"))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")));
        MetaData md = mdBuilder.build();

        IndicesOptions options = IndicesOptions.fromOptions(true, false, true, false);

        String[] results = md.concreteIndices(Strings.EMPTY_ARRAY, options);
        assertEquals(3, results.length);

        results = md.concreteIndices(new String[]{"foo"}, options);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        results = md.concreteIndices(new String[]{"bar"}, options);
        assertThat(results, emptyArray());

        try {
            md.concreteIndices(new String[]{"baz*"}, options);
            fail();
        } catch (IndexMissingException e) {}

        try {
            md.concreteIndices(new String[]{"foo", "baz*"}, options);
            fail();
        } catch (IndexMissingException e) {}
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
        String[] results = md.concreteIndices(Strings.EMPTY_ARRAY, options);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        results = md.concreteIndices(new String[]{"foo*"}, options);
        assertEquals(1, results.length);
        assertEquals("foo", results[0]);

        // no wildcards, so wildcard expansion don't apply
        results = md.concreteIndices(new String[]{"bar"}, options);
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        // Only open
        options = IndicesOptions.fromOptions(false, true, true, false);
        results = md.concreteIndices(Strings.EMPTY_ARRAY, options);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar"));

        results = md.concreteIndices(new String[]{"foo*"}, options);
        assertEquals(1, results.length);
        assertEquals("foobar", results[0]);

        results = md.concreteIndices(new String[]{"bar"}, options);
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        // Open and closed
        options = IndicesOptions.fromOptions(false, true, true, true);
        results = md.concreteIndices(Strings.EMPTY_ARRAY, options);
        assertEquals(3, results.length);
        assertThat(results, arrayContainingInAnyOrder("bar", "foobar", "foo"));

        results = md.concreteIndices(new String[]{"foo*"}, options);
        assertEquals(2, results.length);
        assertThat(results, arrayContainingInAnyOrder("foobar", "foo"));

        results = md.concreteIndices(new String[]{"bar"}, options);
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = md.concreteIndices(new String[]{"-foo*"}, options);
        assertEquals(1, results.length);
        assertEquals("bar", results[0]);

        results = md.concreteIndices(new String[]{"-*"}, options);
        assertEquals(0, results.length);

        options = IndicesOptions.fromOptions(false, false, true, true);
        try {
            md.concreteIndices(new String[]{"-*"}, options);
            fail();
        } catch (IndexMissingException e) {}
    }

    @Test
    public void testIndexOptions_emptyCluster() {
        MetaData md = MetaData.builder().build();
        IndicesOptions options = IndicesOptions.strict();

        String[] results = md.concreteIndices(Strings.EMPTY_ARRAY, options);
        assertThat(results, emptyArray());
        try {
            md.concreteIndices(new String[]{"foo"}, options);
            fail();
        } catch (IndexMissingException e) {}
        results = md.concreteIndices(new String[]{"foo*"}, options);
        assertThat(results, emptyArray());
        try {
            md.concreteIndices(new String[]{"foo*", "bar"}, options);
            fail();
        } catch (IndexMissingException e) {}


        options = IndicesOptions.lenient();
        results = md.concreteIndices(Strings.EMPTY_ARRAY, options);
        assertThat(results, emptyArray());
        results = md.concreteIndices(new String[]{"foo"}, options);
        assertThat(results, emptyArray());
        results = md.concreteIndices(new String[]{"foo*"}, options);
        assertThat(results, emptyArray());
        results = md.concreteIndices(new String[]{"foo*", "bar"}, options);
        assertThat(results, emptyArray());

        options = IndicesOptions.fromOptions(true, false, true, false);
        try {
            md.concreteIndices(Strings.EMPTY_ARRAY, options);
        } catch (IndexMissingException e) {}
    }

    @Test
    public void convertWildcardsJustIndicesTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("testXYY"))
                .put(indexBuilder("testYYY"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testXXX"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testXXX", "testYYY"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testXXX", "ku*"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX", "kuku")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"test*"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testX*"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testX*", "kuku"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX", "testXYY", "kuku")));
    }

    @Test
    public void convertWildcardsTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX").putAlias(AliasMetaData.builder("alias1")).putAlias(AliasMetaData.builder("alias2")))
                .put(indexBuilder("testXYY").putAlias(AliasMetaData.builder("alias2")))
                .put(indexBuilder("testYYY").putAlias(AliasMetaData.builder("alias3")))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testYY*", "alias*"}, IndicesOptions.lenient())), equalTo(newHashSet("alias1", "alias2", "alias3", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"-kuku"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"+test*", "-testYYY"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"+testX*", "+testYYY"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"+testYYY", "+testX*"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
    }

    private IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(ImmutableSettings.settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }

    @Test(expected = IndexMissingException.class)
    public void concreteIndicesIgnoreIndicesOneMissingIndex() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        md.concreteIndices(new String[]{"testZZZ"}, IndicesOptions.strict());
    }

    @Test
    public void concreteIndicesIgnoreIndicesOneMissingIndexOtherFound() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(new String[]{"testXXX", "testZZZ"}, IndicesOptions.lenient())), equalTo(newHashSet("testXXX")));
    }

    @Test(expected = IndexMissingException.class)
    public void concreteIndicesIgnoreIndicesAllMissing() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(new String[]{"testMo", "testMahdy"}, IndicesOptions.strict())), equalTo(newHashSet("testXXX")));
    }

    @Test
    public void concreteIndicesIgnoreIndicesEmptyRequest() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(new String[]{}, IndicesOptions.lenient())), equalTo(Sets.<String>newHashSet("kuku", "testXXX")));
    }

    @Test
    public void testIsAllIndices_null() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isAllIndices(null), equalTo(true));
    }

    @Test
    public void testIsAllIndices_empty() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isAllIndices(new String[0]), equalTo(true));
    }

    @Test
    public void testIsAllIndices_explicitAll() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isAllIndices(new String[]{"_all"}), equalTo(true));
    }

    @Test
    public void testIsAllIndices_explicitAllPlusOther() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isAllIndices(new String[]{"_all", "other"}), equalTo(false));
    }

    @Test
    public void testIsAllIndices_normalIndexes() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isAllIndices(new String[]{"index1", "index2", "index3"}), equalTo(false));
    }

    @Test
    public void testIsAllIndices_wildcard() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isAllIndices(new String[]{"*"}), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_null() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllPattern(null), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_empty() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllPattern(new String[0]), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_explicitAll() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllPattern(new String[]{"_all"}), equalTo(true));
    }

    @Test
    public void testIsExplicitAllIndices_explicitAllPlusOther() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllPattern(new String[]{"_all", "other"}), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_normalIndexes() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllPattern(new String[]{"index1", "index2", "index3"}), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_wildcard() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllPattern(new String[]{"*"}), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_explicitList() throws Exception {
        //even though it does identify all indices, it's not a pattern but just an explicit list of them
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        String[] indicesOrAliases = concreteIndices;
        String[] allConcreteIndices = concreteIndices;
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(indicesOrAliases, concreteIndices), equalTo(false));
    }

    @Test
    public void testIsPatternMatchingAllIndices_onlyWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        String[] allConcreteIndices = concreteIndices;
        MetaData metaData = metaDataBuilder(allConcreteIndices);
        assertThat(metaData.isPatternMatchingAllIndices(indicesOrAliases, concreteIndices), equalTo(true));
    }

    @Test
    public void testIsPatternMatchingAllIndices_matchingTrailingWildcard() throws Exception {
        String[] indicesOrAliases = new String[]{"index*"};
        String[] concreteIndices = new String[]{"index1", "index2", "index3"};
        String[] allConcreteIndices = concreteIndices;
        MetaData metaData = metaDataBuilder(allConcreteIndices);
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
        String[] allConcreteIndices = concreteIndices;
        MetaData metaData = metaDataBuilder(allConcreteIndices);
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
        String[] allConcreteIndices = concreteIndices;
        MetaData metaData = metaDataBuilder(allConcreteIndices);
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

    private MetaData metaDataBuilder(String... indices) {
        MetaData.Builder mdBuilder = MetaData.builder();
        for (String concreteIndex : indices) {
            mdBuilder.put(indexBuilder(concreteIndex));
        }
        return mdBuilder.build();
    }
}
