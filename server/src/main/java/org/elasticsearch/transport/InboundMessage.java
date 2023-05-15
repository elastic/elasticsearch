/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.Objects;

public class InboundMessage extends AbstractRefCounted {

    private final Header header;
    private final ReleasableBytesReference content;
    private final Exception exception;
    private final boolean isPing;
    private Releasable breakerRelease;
    private StreamInput streamInput;

    public InboundMessage(Header header, ReleasableBytesReference content, Releasable breakerRelease) {
        this.header = header;
        this.content = content;
        this.breakerRelease = breakerRelease;
        this.exception = null;
        this.isPing = false;
    }

    public InboundMessage(Header header, Exception exception) {
        this.header = header;
        this.content = null;
        this.breakerRelease = null;
        this.exception = exception;
        this.isPing = false;
    }

    public InboundMessage(Header header, boolean isPing) {
        this.header = header;
        this.content = null;
        this.breakerRelease = null;
        this.exception = null;
        this.isPing = isPing;
    }

    public Header getHeader() {
        return header;
    }

    public int getContentLength() {
        if (content == null) {
            return 0;
        } else {
            return content.length();
        }
    }

    public Exception getException() {
        return exception;
    }

    public boolean isPing() {
        return isPing;
    }

    public boolean isShortCircuit() {
        return exception != null;
    }

    public Releasable takeBreakerReleaseControl() {
        final Releasable toReturn = breakerRelease;
        breakerRelease = null;
        return Objects.requireNonNullElse(toReturn, () -> {});
    }

    public StreamInput openOrGetStreamInput() throws IOException {
        assert isPing == false && content != null;
        assert hasReferences();
        if (streamInput == null) {
            streamInput = content.streamInput();
            streamInput.setTransportVersion(header.getVersion());
        }
        return streamInput;
    }

    @Override
    public String toString() {
        return "InboundMessage{" + header + "}";
    }

    @Override
    protected void closeInternal() {
        try {
            IOUtils.close(streamInput, content, breakerRelease);
        } catch (Exception e) {
            assert false : e;
            throw new ElasticsearchException(e);
        }
    }
}
