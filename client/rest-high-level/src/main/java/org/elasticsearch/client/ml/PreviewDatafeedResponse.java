/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Response containing a datafeed preview in JSON format
 */
public class PreviewDatafeedResponse implements ToXContentObject {

    private BytesReference preview;

    public static PreviewDatafeedResponse fromXContent(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            parser.nextToken();
            builder.copyCurrentStructure(parser);
            return new PreviewDatafeedResponse(BytesReference.bytes(builder));
        }
    }

    public PreviewDatafeedResponse(BytesReference preview) {
        this.preview = preview;
    }

    public BytesReference getPreview() {
        return preview;
    }

    /**
     * Parses the preview to a list of {@link Map} objects
     * @return List of previewed data
     * @throws IOException If there is a parsing issue with the {@link BytesReference}
     * @throws java.lang.ClassCastException If casting the raw {@link Object} entries to a {@link Map} fails
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getDataList() throws IOException {
        try(StreamInput streamInput = preview.streamInput();
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, streamInput)) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.START_ARRAY) {
                return parser.listOrderedMap().stream().map(obj -> (Map<String, Object>)obj).collect(Collectors.toList());
            } else {
                return Collections.singletonList(parser.mapOrdered());
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        try (InputStream stream = preview.streamInput()) {
            builder.rawValue(stream, XContentType.JSON);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(preview);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PreviewDatafeedResponse other = (PreviewDatafeedResponse) obj;
        return Objects.equals(preview, other.preview);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }
}
