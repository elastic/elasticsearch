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
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.equalTo;

public class WildcardExpressionResolverTests extends ESTestCase {
    public void testConvertWildcardsJustIndicesTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("testXYY"))
                .put(indexBuilder("testYYY"))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testXXX"))), equalTo(newHashSet("testXXX")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testXXX", "testYYY"))), equalTo(newHashSet("testXXX", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testXXX", "ku*"))), equalTo(newHashSet("testXXX", "kuku")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("test*"))), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testX*"))), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testX*", "kuku"))), equalTo(newHashSet("testXXX", "testXYY", "kuku")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("*"))), equalTo(newHashSet("testXXX", "testXYY", "testYYY", "kuku")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("*", "-kuku"))), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
    }

    public void testConvertWildcardsTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX").putAlias(AliasMetaData.builder("alias1")).putAlias(AliasMetaData.builder("alias2")))
                .put(indexBuilder("testXYY").putAlias(AliasMetaData.builder("alias2")))
                .put(indexBuilder("testYYY").putAlias(AliasMetaData.builder("alias3")))
                .put(indexBuilder("kuku"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testYY*", "alias*"))), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("-kuku"))), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("+test*", "-testYYY"))), equalTo(newHashSet("testXXX", "testXYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("+testX*", "+testYYY"))), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("+testYYY", "+testX*"))), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
    }

    public void testConvertWildcardsOpenClosedIndicesTests() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX").state(IndexMetaData.State.OPEN))
                .put(indexBuilder("testXXY").state(IndexMetaData.State.OPEN))
                .put(indexBuilder("testXYY").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("testYYY").state(IndexMetaData.State.OPEN))
                .put(indexBuilder("testYYX").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("kuku").state(IndexMetaData.State.OPEN));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, true, true));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testX*"))), equalTo(newHashSet("testXXX", "testXXY", "testXYY")));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, false, true));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testX*"))), equalTo(newHashSet("testXYY")));
        context = new IndexNameExpressionResolver.Context(state, IndicesOptions.fromOptions(true, true, true, false));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("testX*"))), equalTo(newHashSet("testXXX", "testXXY")));
    }

    // issue #13334
    public void testMultipleWildcards() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("testXXX"))
                .put(indexBuilder("testXXY"))
                .put(indexBuilder("testXYY"))
                .put(indexBuilder("testYYY"))
                .put(indexBuilder("kuku"))
                .put(indexBuilder("kukuYYY"));

        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("test*X*"))), equalTo(newHashSet("testXXX", "testXXY", "testXYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("test*X*Y"))), equalTo(newHashSet("testXXY", "testXYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("kuku*Y*"))), equalTo(newHashSet("kukuYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("*Y*"))), equalTo(newHashSet("testXXY", "testXYY", "testYYY", "kukuYYY")));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("test*Y*X"))).size(), equalTo(0));
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("*Y*X"))).size(), equalTo(0));
    }

    public void testAll() {
        MetaData.Builder mdBuilder = MetaData.builder()
            .put(indexBuilder("testXXX"))
            .put(indexBuilder("testXYY"))
            .put(indexBuilder("testYYY"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        IndexNameExpressionResolver.WildcardExpressionResolver resolver = new IndexNameExpressionResolver.WildcardExpressionResolver();

        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(state, IndicesOptions.lenientExpandOpen());
        assertThat(newHashSet(resolver.resolve(context, Arrays.asList("_all"))), equalTo(newHashSet("testXXX", "testXYY", "testYYY")));
    }

    private IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(settings(Version.CURRENT).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }

}
