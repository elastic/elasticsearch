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

import java.io.IOException;

public class AggregatedMessage {

    private final Header header;
    private final ReleasableBytesReference content;
    private final boolean isPing;
    private StreamInput streamInput;

    public AggregatedMessage(Header header, ReleasableBytesReference content) {
        this.header = header;
        this.content = content;
        this.isPing = false;
    }

    public AggregatedMessage(Header header, boolean isPing) {
        this.header = header;
        this.content = null;
        this.isPing = isPing;
    }

    public Header getHeader() {
        return header;
    }

    public ReleasableBytesReference getContent() {
        return content;
    }

    public boolean isPing() {
        return isPing;
    }

    public StreamInput openOrGetStreamInput() throws IOException {
        assert isPing == false && content != null;
        if (streamInput == null) {
            streamInput = content.streamInput();
            streamInput.setVersion(header.getVersion());
        }
        return streamInput;
    }
}
