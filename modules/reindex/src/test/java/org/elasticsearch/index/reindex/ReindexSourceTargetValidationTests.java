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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.remote.RemoteInfo;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests source and target index validation of reindex. Mostly that means testing that indexing from an index back into itself fails the
 * request. Note that we can't catch you trying to remotely reindex from yourself into yourself. We actually assert here that reindexes
 * from remote don't need to come from existing indexes. It'd be silly to fail requests if the source index didn't exist on the target
 * cluster....
 */
public class ReindexSourceTargetValidationTests extends ESTestCase {
    private static final ClusterState STATE = ClusterState.builder(new ClusterName("test")).metaData(MetaData.builder()
                .put(index("target", "target_alias", "target_multi"), true)
                .put(index("target2", "target_multi"), true)
                .put(index("foo"), true)
                .put(index("bar"), true)
                .put(index("baz"), true)
                .put(index("source", "source_multi"), true)
                .put(index("source2", "source_multi"), true)).build();
    private static final IndexNameExpressionResolver INDEX_NAME_EXPRESSION_RESOLVER = new IndexNameExpressionResolver(Settings.EMPTY);
    private static final AutoCreateIndex AUTO_CREATE_INDEX = new AutoCreateIndex(Settings.EMPTY, INDEX_NAME_EXPRESSION_RESOLVER);

    public void testObviousCases() {
        fails("target", "target");
        fails("target", "foo", "bar", "target", "baz");
        fails("target", "foo", "bar", "target", "baz", "target");
        succeeds("target", "source");
        succeeds("target", "source", "source2");
    }

    public void testAliasesContainTarget() {
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

    public void testTargetIsAlias() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> succeeds("target_multi", "foo"));
        assertThat(e.getMessage(), containsString("Alias [target_multi] has more than one indices associated with it [["));
        // The index names can come in either order
        assertThat(e.getMessage(), containsString("target"));
        assertThat(e.getMessage(), containsString("target2"));
    }

    public void testRemoteInfoSkipsValidation() {
        // The index doesn't have to exist
        succeeds(new RemoteInfo(randomAsciiOfLength(5), "test", 9200, new BytesArray("test"), null, null, emptyMap()), "does_not_exist",
                "target");
        // And it doesn't matter if they are the same index. They are considered to be different because the remote one is, well, remote.
        succeeds(new RemoteInfo(randomAsciiOfLength(5), "test", 9200, new BytesArray("test"), null, null, emptyMap()), "target", "target");
    }

    private void fails(String target, String... sources) {
        Exception e = expectThrows(ActionRequestValidationException.class, () -> succeeds(target, sources));
        assertThat(e.getMessage(), containsString("reindex cannot write into an index its reading from [target]"));
    }

    private void succeeds(String target, String... sources) {
        succeeds(null, target, sources);
    }

    private void succeeds(RemoteInfo remoteInfo, String target, String... sources) {
        TransportReindexAction.validateAgainstAliases(new SearchRequest(sources), new IndexRequest(target), remoteInfo,
                INDEX_NAME_EXPRESSION_RESOLVER, AUTO_CREATE_INDEX, STATE);
    }

    private static IndexMetaData index(String name, String... aliases) {
        IndexMetaData.Builder builder = IndexMetaData.builder(name).settings(Settings.builder()
                .put("index.version.created", Version.CURRENT.id)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1));
        for (String alias: aliases) {
            builder.putAlias(AliasMetaData.builder(alias).build());
        }
        return builder.build();
    }
}
