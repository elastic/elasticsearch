/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.inference.completion.ContentObject.ContentObjectType.TEXT;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.CONTENT_FIELD;

public record ContentObjects(List<ContentObject> contentObjects) implements Content, NamedWriteable {

    public static final String NAME = "content_objects";

    public ContentObjects(StreamInput in) throws IOException {
        this(in.readNamedWriteableCollectionAsList(ContentObject.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableCollection(contentObjects);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CONTENT_FIELD, contentObjects);
    }

    @Override
    public boolean containsMultimodalContent() {
        return contentObjects.stream().anyMatch(o -> o.type().equals(TEXT) == false);
    }
}
