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

package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.util.Objects;

public class InboundMessage implements Releasable {

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
        if (streamInput == null) {
            streamInput = content.streamInput();
            streamInput.setVersion(header.getVersion());
        }
        return streamInput;
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(streamInput);
        Releasables.closeWhileHandlingException(content, breakerRelease);
    }
}
