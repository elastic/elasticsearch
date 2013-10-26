/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class MetaDataTests extends ElasticsearchTestCase {

    @Test
    public void convertWildcardsJustIndicesTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("testXYY"))
                .put(indexBuilder("testYYY"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testXXX"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testXXX", "testYYY"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testXXX", "ku*"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX", "kuku")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"test*"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testX*"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testX*", "kuku"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX", "testXYY", "kuku")));
    }

    @Test
    public void convertWildcardsTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX").putAlias(AliasMetaData.builder("alias1")).putAlias(AliasMetaData.builder("alias2")))
                .put(indexBuilder("testXYY").putAlias(AliasMetaData.builder("alias2")))
                .put(indexBuilder("testYYY").putAlias(AliasMetaData.builder("alias3")))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"testYY*", "alias*"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("alias1", "alias2", "alias3", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"-kuku"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"+test*", "-testYYY"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"+testX*", "+testYYY"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(md.convertFromWildcards(new String[]{"+testYYY", "+testX*"}, true, IgnoreIndices.MISSING)), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
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
        md.concreteIndices(new String[]{"testZZZ"}, IgnoreIndices.MISSING, true);
    }

    @Test
    public void concreteIndicesIgnoreIndicesOneMissingIndexOtherFound() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(new String[]{"testXXX", "testZZZ"}, IgnoreIndices.MISSING, true)), equalTo(newHashSet("testXXX")));
    }

    @Test(expected = IndexMissingException.class)
    public void concreteIndicesIgnoreIndicesAllMissing() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(new String[]{"testMo", "testMahdy"}, IgnoreIndices.MISSING, true)), equalTo(newHashSet("testXXX")));
    }

    @Test
    public void concreteIndicesIgnoreIndicesEmptyRequest() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("kuku"));
        MetaData md = mdBuilder.build();
        assertThat(newHashSet(md.concreteIndices(new String[]{}, IgnoreIndices.MISSING, true)), equalTo(Sets.<String>newHashSet("kuku", "testXXX")));
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
        assertThat(metaData.isExplicitAllIndices(null), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_empty() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllIndices(new String[0]), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_explicitAll() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllIndices(new String[]{"_all"}), equalTo(true));
    }

    @Test
    public void testIsExplicitAllIndices_explicitAllPlusOther() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllIndices(new String[]{"_all", "other"}), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_normalIndexes() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllIndices(new String[]{"index1", "index2", "index3"}), equalTo(false));
    }

    @Test
    public void testIsExplicitAllIndices_wildcard() throws Exception {
        MetaData metaData = MetaData.builder().build();
        assertThat(metaData.isExplicitAllIndices(new String[]{"*"}), equalTo(false));
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
