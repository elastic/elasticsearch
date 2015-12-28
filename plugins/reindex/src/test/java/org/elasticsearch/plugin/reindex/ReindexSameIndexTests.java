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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.junit.Before;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that indexing from an index back into itself fails the request.
 */
public class ReindexSameIndexTests extends ReindexTestCase {
    @Before
    public void createIndices() {
        createIndex("target", "target2", "foo", "bar", "baz", "source", "source2");
        ensureGreen();
    }

    public void testObviousCases() throws Exception {
        fails("target", "target");
        fails("target", "foo", "bar", "target", "baz");
        fails("target", "foo", "bar", "target", "baz", "target");
        succeeds("target", "source");
        succeeds("target", "source", "source2");
    }

    public void testAliasesContainTarget() throws Exception {
        assertAcked(client().admin().indices().prepareAliases()
                .addAlias("target", "target_alias")
                .addAlias(new String[] {"target", "target2"}, "target_multi")
                .addAlias(new String[] {"source", "source2"}, "source_multi"));

        fails("target", "target_alias");
        fails("target_alias", "target");
        fails("target", "foo", "bar", "target_alias", "baz");
        fails("target_alias", "foo", "bar", "target_alias", "baz");
        fails("target_alias", "foo", "bar", "target", "baz");
        fails("target", "foo", "bar", "target_alias", "target_alias");
        fails("target", "target_multi");
        fails("target", "foo", "bar", "target_multi", "baz");
        succeeds("target", "source_multi");
        succeeds("target", "source", "source2", "source_multi");
    }

    public void testTargetIsAlias() throws Exception {
        assertAcked(client().admin().indices().prepareAliases()
                .addAlias(new String[] {"target", "target2"}, "target_multi"));

        try {
            reindex().source("foo").destination("target_multi").get();
            fail("Expected failure");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Alias [target_multi] has more than one indices associated with it [["));
            // The index names can come in either order
            assertThat(e.getMessage(), containsString("target"));
            assertThat(e.getMessage(), containsString("target2"));
        }
    }

    private void fails(String target, String... sources) throws Exception {
        try {
            reindex().source(sources).destination(target).get();
            fail("Expected an exception");
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(),
                    containsString("index-by-search cannot write into an index its reading from [target]"));
        }
    }

    private void succeeds(String target, String... sources) throws Exception {
        reindex().source(sources).destination(target).get();
    }
}
