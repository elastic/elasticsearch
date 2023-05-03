/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
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
    private static final ClusterState STATE = ClusterState.builder(new ClusterName("test"))
        .metadata(
            Metadata.builder()
                .put(index("target", "target_alias", "target_multi"), true)
                .put(index("target2", "target_multi"), true)
                .put(index("target_with_write_index", true, "target_multi_with_write_index"), true)
                .put(index("target2_without_write_index", "target_multi_with_write_index"), true)
                .put(index("qux", false, "target_alias_with_write_index_disabled"), true)
                .put(index("foo"), true)
                .put(index("bar"), true)
                .put(index("baz"), true)
                .put(index("source", "source_multi"), true)
                .put(index("source2", "source_multi"), true)
        )
        .build();
    private static final IndexNameExpressionResolver INDEX_NAME_EXPRESSION_RESOLVER = TestIndexNameExpressionResolver.newInstance();
    private static final AutoCreateIndex AUTO_CREATE_INDEX = new AutoCreateIndex(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
        INDEX_NAME_EXPRESSION_RESOLVER,
        EmptySystemIndices.INSTANCE
    );

    private final BytesReference query = new BytesArray("{ \"foo\" : \"bar\" }");

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

    public void testTargetIsAliasToMultipleIndicesWithoutWriteAlias() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> succeeds("target_multi", "foo"));
        assertThat(
            e.getMessage(),
            containsString(
                "no write index is defined for alias [target_multi]. The write index may be explicitly "
                    + "disabled using is_write_index=false or the alias points to multiple indices without one being designated as a "
                    + "write index"
            )
        );
    }

    public void testTargetIsAliasWithWriteIndexDisabled() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> succeeds("target_alias_with_write_index_disabled", "foo"));
        assertThat(
            e.getMessage(),
            containsString(
                "no write index is defined for alias [target_alias_with_write_index_disabled]. "
                    + "The write index may be explicitly disabled using is_write_index=false or the alias points to multiple "
                    + "indices without one being designated as a write index"
            )
        );
        succeeds("qux", "foo"); // writing directly into the index of which this is the alias works though
    }

    public void testTargetIsWriteAlias() {
        succeeds("target_multi_with_write_index", "foo");
        succeeds("target_multi_with_write_index", "target2_without_write_index");
        fails("target_multi_with_write_index", "target_multi_with_write_index");
        fails("target_multi_with_write_index", "target_with_write_index");
    }

    public void testRemoteInfoSkipsValidation() {
        // The index doesn't have to exist
        succeeds(
            new RemoteInfo(
                randomAlphaOfLength(5),
                "test",
                9200,
                null,
                query,
                null,
                null,
                emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            ),
            "does_not_exist",
            "target"
        );
        // And it doesn't matter if they are the same index. They are considered to be different because the remote one is, well, remote.
        succeeds(
            new RemoteInfo(
                randomAlphaOfLength(5),
                "test",
                9200,
                null,
                query,
                null,
                null,
                emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            ),
            "target",
            "target"
        );
    }

    private void fails(String target, String... sources) {
        Exception e = expectThrows(ActionRequestValidationException.class, () -> succeeds(target, sources));
        assertThat(e.getMessage(), containsString("reindex cannot write into an index its reading from"));
    }

    private void succeeds(String target, String... sources) {
        succeeds(null, target, sources);
    }

    private void succeeds(RemoteInfo remoteInfo, String target, String... sources) {
        ReindexValidator.validateAgainstAliases(
            new SearchRequest(sources),
            new IndexRequest(target),
            remoteInfo,
            INDEX_NAME_EXPRESSION_RESOLVER,
            AUTO_CREATE_INDEX,
            STATE
        );
    }

    private static IndexMetadata index(String name, String... aliases) {
        return index(name, null, aliases);
    }

    private static IndexMetadata index(String name, @Nullable Boolean writeIndex, String... aliases) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name).settings(indexSettings(Version.CURRENT, 1, 1));
        for (String alias : aliases) {
            builder.putAlias(AliasMetadata.builder(alias).writeIndex(writeIndex).build());
        }
        return builder.build();
    }
}
