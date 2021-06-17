/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class SnapshotRequestsTests extends ESTestCase {
    public void testRestoreSnapshotRequestParsing() throws IOException {
        RestoreSnapshotRequest request = new RestoreSnapshotRequest("test-repo", "test-snap");

        XContentBuilder builder = jsonBuilder().startObject();

        if (randomBoolean()) {
            builder.field("indices", "foo,bar,baz");
        } else {
            builder.startArray("indices");
            builder.value("foo");
            builder.value("bar");
            builder.value("baz");
            builder.endArray();
        }

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        if (indicesOptions.expandWildcardsClosed()) {
            if (indicesOptions.expandWildcardsOpen()) {
                builder.field("expand_wildcards", "all");
            } else {
                builder.field("expand_wildcards", "closed");
            }
        } else {
            if (indicesOptions.expandWildcardsOpen()) {
                builder.field("expand_wildcards", "open");
            } else {
                builder.field("expand_wildcards", "none");
            }
        }
        builder.field("allow_no_indices", indicesOptions.allowNoIndices());
        builder.field("rename_pattern", "rename-from");
        builder.field("rename_replacement", "rename-to");
        boolean partial = randomBoolean();
        builder.field("partial", partial);
        builder.startObject("index_settings").field("set1", "val2").endObject();
        if (randomBoolean()) {
            builder.field("ignore_index_settings", "set2,set3");
        } else {
            builder.startArray("ignore_index_settings");
            builder.value("set2");
            builder.value("set3");
            builder.endArray();
        }
        boolean includeIgnoreUnavailable = randomBoolean();
        if (includeIgnoreUnavailable) {
            builder.field("ignore_unavailable", indicesOptions.ignoreUnavailable());
        }

        BytesReference bytes = BytesReference.bytes(builder.endObject());

        request.source(XContentHelper.convertToMap(bytes, true, builder.contentType()).v2());

        assertEquals("test-repo", request.repository());
        assertEquals("test-snap", request.snapshot());
        assertArrayEquals(request.indices(), new String[] { "foo", "bar", "baz" });
        assertEquals("rename-from", request.renamePattern());
        assertEquals("rename-to", request.renameReplacement());
        assertEquals(partial, request.partial());
        assertArrayEquals(request.ignoreIndexSettings(), new String[] { "set2", "set3" });
        boolean expectedIgnoreAvailable = includeIgnoreUnavailable
            ? indicesOptions.ignoreUnavailable()
            : IndicesOptions.strictExpandOpen().ignoreUnavailable();
        assertEquals(expectedIgnoreAvailable, request.indicesOptions().ignoreUnavailable());
    }

    public void testCreateSnapshotRequestParsing() throws IOException {
        CreateSnapshotRequest request = new CreateSnapshotRequest("test-repo", "test-snap");

        XContentBuilder builder = jsonBuilder().startObject();

        if (randomBoolean()) {
            builder.field("indices", "foo,bar,baz");
        } else {
            builder.startArray("indices");
            builder.value("foo");
            builder.value("bar");
            builder.value("baz");
            builder.endArray();
        }

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        if (indicesOptions.expandWildcardsClosed()) {
            if (indicesOptions.expandWildcardsOpen()) {
                builder.field("expand_wildcards", "all");
            } else {
                builder.field("expand_wildcards", "closed");
            }
        } else {
            if (indicesOptions.expandWildcardsOpen()) {
                builder.field("expand_wildcards", "open");
            } else {
                builder.field("expand_wildcards", "none");
            }
        }
        builder.field("allow_no_indices", indicesOptions.allowNoIndices());
        boolean partial = randomBoolean();
        builder.field("partial", partial);
        builder.startObject("index_settings").field("set1", "val2").endObject();
        if (randomBoolean()) {
            builder.field("ignore_index_settings", "set2,set3");
        } else {
            builder.startArray("ignore_index_settings");
            builder.value("set2");
            builder.value("set3");
            builder.endArray();
        }
        boolean includeIgnoreUnavailable = randomBoolean();
        if (includeIgnoreUnavailable) {
            builder.field("ignore_unavailable", indicesOptions.ignoreUnavailable());
        }

        BytesReference bytes = BytesReference.bytes(builder.endObject());

        request.source(XContentHelper.convertToMap(bytes, true, builder.contentType()).v2());

        assertEquals("test-repo", request.repository());
        assertEquals("test-snap", request.snapshot());
        assertArrayEquals(request.indices(), new String[] { "foo", "bar", "baz" });
        assertEquals(partial, request.partial());
        boolean expectedIgnoreAvailable = includeIgnoreUnavailable
            ? indicesOptions.ignoreUnavailable()
            : IndicesOptions.strictExpandOpen().ignoreUnavailable();
        assertEquals(expectedIgnoreAvailable, request.indicesOptions().ignoreUnavailable());
    }

}
