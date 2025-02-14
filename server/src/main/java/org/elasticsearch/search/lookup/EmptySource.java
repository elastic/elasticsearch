/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

final class EmptySource implements Source {

    private static final EnumMap<XContentType, EmptySource> values = new EnumMap<>(XContentType.class);

    static {
        for (XContentType value : XContentType.values()) {
            values.put(value, new EmptySource(value));
        }
    }

    static EmptySource forType(XContentType type) {
        return values.get(type);
    }

    private final XContentType type;

    private final BytesReference sourceRef;

    private EmptySource(XContentType type) {
        this.type = type;
        try {
            sourceRef = new BytesArray(
                BytesReference.toBytes(BytesReference.bytes(new XContentBuilder(type.xContent(), new BytesStreamOutput()).value(Map.of())))
            );
        } catch (IOException e) {
            throw new AssertionError("impossible", e);
        }
    }

    @Override
    public XContentType sourceContentType() {
        return type;
    }

    @Override
    public Map<String, Object> source() {
        return Map.of();
    }

    @Override
    public BytesReference internalSourceRef() {
        return sourceRef;
    }

    @Override
    public Source filter(SourceFilter sourceFilter) {
        return this;
    }
}
