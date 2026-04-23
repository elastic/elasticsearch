/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SearchContextMissingNodesExceptionTests extends ESTestCase {

    public void testScrollExceptionMessage() {
        Set<String> missingNodes = Set.of("node1", "node2");
        SearchContextMissingNodesException ex = new SearchContextMissingNodesException(
            SearchContextMissingNodesException.ContextType.SCROLL,
            missingNodes
        );

        assertThat(ex.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(ex.getContextType(), equalTo(SearchContextMissingNodesException.ContextType.SCROLL));
        assertThat(ex.getMissingNodeIds(), equalTo(missingNodes));

        assertThat(ex.getMessage(), containsString("Search context of type [scroll] references nodes that have left the cluster:"));
    }

    public void testSerialization() throws IOException {
        Set<String> missingNodes = Set.of("node1", "node2", "node3");
        SearchContextMissingNodesException original = new SearchContextMissingNodesException(
            SearchContextMissingNodesException.ContextType.SCROLL,
            missingNodes
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        SearchContextMissingNodesException deserialized = new SearchContextMissingNodesException(in);

        assertThat(deserialized.getContextType(), equalTo(original.getContextType()));
        assertThat(deserialized.getMissingNodeIds(), equalTo(original.getMissingNodeIds()));
        assertThat(deserialized.status(), equalTo(original.status()));
        assertThat(deserialized.getMessage(), equalTo(original.getMessage()));
    }

    public void testToXContent() throws IOException {
        Set<String> missingNodes = Set.of("node1", "node2");
        SearchContextMissingNodesException ex = new SearchContextMissingNodesException(
            SearchContextMissingNodesException.ContextType.SCROLL,
            missingNodes
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = BytesReference.bytes(builder).utf8ToString();

        assertThat(json, containsString("\"context_type\":\"scroll\""));
        assertThat(json, containsString("\"missing_nodes\""));
        assertThat(json, containsString("node1"));
        assertThat(json, containsString("node2"));
    }

    public void testStatus() {
        SearchContextMissingNodesException ex = new SearchContextMissingNodesException(
            SearchContextMissingNodesException.ContextType.SCROLL,
            Set.of("node1")
        );
        assertThat(ex.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(ex.status().getStatus(), equalTo(404));
    }
}
