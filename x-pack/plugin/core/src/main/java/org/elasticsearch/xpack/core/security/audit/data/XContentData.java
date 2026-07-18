/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

/**
 * Bridges existing {@link org.elasticsearch.xcontent.ToXContent} serialization into the {@link DataValue} model.
 * <p>
 * Rather than re-implementing the (often intricate) serialization of complex domain objects as direct
 * {@link DataObject} construction, this reuses their {@code toXContent} output: the object is rendered to JSON, parsed
 * back into an ordered generic map, and walked into a {@link DataObject} via {@link DataValues}. This keeps a single,
 * already-tested description of each object's JSON shape while yielding a typed, inspectable tree.
 * <p>
 * The intermediate map is always parsed <em>ordered</em>, so the resulting {@link DataObject} preserves field order and
 * downstream encoding stays deterministic. Builders are created with {@code humanReadable(true)} to match the audit
 * trail's long-standing rendering of human-readable values (for example durations and byte sizes).
 */
public final class XContentData {

    private XContentData() {}

    /**
     * Serializes a self-contained {@link ToXContentObject} and converts the result into a {@link DataObject}.
     *
     * @param content a value whose {@code toXContent} writes a complete JSON object
     * @param params  the parameters to pass through to {@code toXContent}
     * @return the object as a {@link DataObject}
     * @throws IOException if serialization fails
     */
    public static DataObject fromXContent(ToXContentObject content, ToXContent.Params params) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true)) {
            content.toXContent(builder, params);
            return fromBuilder(builder);
        }
    }

    /**
     * Converts the contents of a completed {@link XContentBuilder} into a {@link DataObject}.
     * <p>
     * The builder must hold a single, finished JSON object (all opened objects/arrays closed). This is the escape hatch
     * for call sites that assemble a bespoke object (opening the builder, writing several fragments, then closing it)
     * rather than serializing a single {@link ToXContentObject}.
     *
     * @param builder a builder positioned at the end of a complete JSON object
     * @return the object as a {@link DataObject}
     */
    public static DataObject fromBuilder(XContentBuilder builder) {
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), true, XContentType.JSON).v2();
        return DataValues.objectFromMap(map);
    }
}
