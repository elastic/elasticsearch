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

package org.elasticsearch.index.reindex;

import org.elasticsearch.plugins.Plugin;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that you can actually cancel a reindex request and all the plumbing works. Doesn't test all of the different cancellation places -
 * that is the responsibility of {@link AsyncBulkByScrollActionTests} which have more precise control to simulate failures but do not
 * exercise important portion of the stack like transport and task management.
 */
public class ReindexCancelTests extends ReindexTestCase {
    public void testCancel() throws Exception {
        BulkIndexByScrollResponse response = CancelTestUtils.testCancel(this, reindex().destination("dest", "test"), ReindexAction.NAME);

        assertThat(response, reindexResponseMatcher().created(1).reasonCancelled(equalTo("by user request")));
        refresh("dest");
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), 1);
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CancelTestUtils.nodePlugins();
    }
}
