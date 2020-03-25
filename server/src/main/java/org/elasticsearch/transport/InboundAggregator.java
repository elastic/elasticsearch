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

    public void headerReceived(Header header) {
        if (currentHeader != null) {
            currentHeader = null;
            throw new IllegalStateException("Header already received.");
        }

        currentHeader = header;
    }

    public void aggregate(ReleasableBytesReference content) {
        if (currentHeader == null) {
            content.close();
            throw new IllegalStateException("Received content without header");
        } else {
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
    }

    public Header cancelAggregation() {
        if (currentHeader == null) {
            throw new IllegalStateException("Aggregation cancelled, but no aggregation had begun");
        } else {
            final Header header = this.currentHeader;
            closeAggregation();
            return header;
        }
    }

    public InboundMessage finishAggregation() throws IOException {
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
        clearAggregation();
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
        closeAggregation();
    }

    private void closeAggregation() {
        if (contentAggregation == null) {
            Releasables.close(firstContent);
        } else {
            Releasables.close(contentAggregation);
        }
        clearAggregation();
    }

    private void clearAggregation() {
        firstContent = null;
        contentAggregation = null;
        currentHeader = null;
    }
}
