/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class IndexResolverTests extends ESTestCase {

    public void testBasicFlow() throws IOException {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(IndexNameExpressionResolverTests.indexBuilder("foo").putAlias(AliasMetadata.builder("foofoobar")))
            .put(IndexNameExpressionResolverTests.indexBuilder("foobar").putAlias(AliasMetadata.builder("foofoobar")))
            .put(IndexNameExpressionResolverTests.indexBuilder("foofoo-closed").state(IndexMetadata.State.CLOSE))
            .put(IndexNameExpressionResolverTests.indexBuilder("foofoo").putAlias(AliasMetadata.builder("barbaz")));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        Directory dir = IndexResolver.buildIndex(state);
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(dir));
        assertThat(searcher.count(new MatchAllDocsQuery()), equalTo(6));
    }

}
