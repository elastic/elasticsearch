/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.index;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

public class SourceContext implements Writeable {

    private XContentType contentType;
    private BytesReference source;
    private Releasable sourceReleasable;

    public SourceContext(XContentType contentType, BytesReference source, Releasable sourceReleasable) {
        this.contentType = contentType;
        this.source = source;
        this.sourceReleasable = sourceReleasable;
    }

    public SourceContext(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            // faster than StreamInput::readEnum, do not replace we read a lot of these instances at times
            contentType = XContentType.ofOrdinal(in.readByte());
        } else {
            contentType = null;
        }
        source = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (contentType != null) {
            out.writeBoolean(true);
            XContentHelper.writeTo(out, contentType);
        } else {
            out.writeBoolean(false);
        }
        out.writeBytesReference(source);
    }

    public XContentType getContentType() {
        return contentType;
    }

    public BytesReference getSource() {
        return source;
    }

    public Map<String, Object> sourceAsMap() {
        return XContentHelper.convertToMap(source, false, contentType).v2();
    }


}
