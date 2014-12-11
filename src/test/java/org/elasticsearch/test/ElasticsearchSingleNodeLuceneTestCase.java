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

package org.elasticsearch.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.After;
import org.junit.Ignore;

/**
 * Like {@link ElasticsearchSingleNodeTest} but for tests that need to extend
 * {@link ElasticsearchLuceneTestCase}.
 */
@Ignore
public abstract class ElasticsearchSingleNodeLuceneTestCase extends ElasticsearchLuceneTestCase {

    @After
    public void cleanup() {
        ElasticsearchSingleNodeTest.cleanup(resetNodeAfterTest());
    }

    /**
     * This method returns <code>true</code> if the node that is used in the background should be reset
     * after each test. This is useful if the test changes the cluster state metadata etc. The default is
     * <code>false</code>.
     */
    protected boolean resetNodeAfterTest() {
        return false;
    }

    /**
     * Create a new index on the singleton node with empty index settings.
     */
    protected static IndexService createIndex(String index) {
        return ElasticsearchSingleNodeTest.createIndex(index);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected static IndexService createIndex(String index, Settings settings) {
        return ElasticsearchSingleNodeTest.createIndex(index, settings);
    }

    /**
     * Create a new search context.
     */
    protected static SearchContext createSearchContext(IndexService indexService) {
        return ElasticsearchSingleNodeTest.createSearchContext(indexService);
    }
}
