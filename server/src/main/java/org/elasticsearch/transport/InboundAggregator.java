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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;

import java.io.IOException;
import java.util.ArrayList;

public class InboundAggregator implements Releasable {

    private ReleasableBytesReference firstContent;
    private ArrayList<ReleasableBytesReference> contentAggregation;
    private Header currentHeader;
    private boolean isClosed = false;

    public void headerReceived(Header header) {
        ensureOpen();
        assert isAggregating() == false;
        assert firstContent == null && contentAggregation == null;
        currentHeader = header;
    }

    public void aggregate(ReleasableBytesReference content) {
        ensureOpen();
        assert isAggregating();
        if (isFirstContent()) {
            firstContent = content.retain();
        } else {
            if (contentAggregation == null) {
                contentAggregation = new ArrayList<>(4);
                contentAggregation.add(firstContent);
                firstContent = null;
            }
            contentAggregation.add(content.retain());
        }
    }

    public Header cancelAggregation() {
        ensureOpen();
        assert isAggregating();
        final Header header = this.currentHeader;
        closeCurrentAggregation();
        return header;
    }

    public InboundMessage finishAggregation() throws IOException {
        ensureOpen();
        final ReleasableBytesReference releasableContent;
        if (isFirstContent()) {
            releasableContent = ReleasableBytesReference.wrap(BytesArray.EMPTY);
        } else if (contentAggregation == null) {
            releasableContent = firstContent;
        } else {
            final ReleasableBytesReference[] references = contentAggregation.toArray(new ReleasableBytesReference[0]);
            final CompositeBytesReference content = new CompositeBytesReference(references);
            releasableContent = new ReleasableBytesReference(content, () -> Releasables.close(references));
        }
        final InboundMessage aggregated = new InboundMessage(currentHeader, releasableContent);
        resetCurrentAggregation();
        boolean success = false;
        try {
            if (aggregated.getHeader().needsToReadVariableHeader()) {
                aggregated.getHeader().finishParsingHeader(aggregated.openOrGetStreamInput());
            }
            success = true;
            return aggregated;
        } finally {
            if (success == false) {
                aggregated.close();
            }
        }
    }

    public boolean isAggregating() {
        return currentHeader != null;
    }

    private boolean isFirstContent() {
        return firstContent == null && contentAggregation == null;
    }

    @Override
    public void close() {
        isClosed = true;
        closeCurrentAggregation();
    }

    private void closeCurrentAggregation() {
        if (contentAggregation == null) {
            Releasables.close(firstContent);
        } else {
            Releasables.close(contentAggregation);
        }
        resetCurrentAggregation();
    }

    private void resetCurrentAggregation() {
        firstContent = null;
        contentAggregation = null;
        currentHeader = null;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Aggregator is already closed");
        }
    }
}
