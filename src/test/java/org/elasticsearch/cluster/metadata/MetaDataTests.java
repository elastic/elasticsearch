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
}
