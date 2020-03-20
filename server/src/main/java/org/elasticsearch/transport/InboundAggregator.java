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

import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;

import java.util.ArrayList;

public class InboundAggregator implements Releasable {

    private final ArrayList<ReleasableBytesReference> contentAggregation = new ArrayList<>();
    private Header currentHeader;

    public void headerReceived(Header header) {
        if (currentHeader != null) {
            currentHeader = null;
            throw new IllegalStateException("Header already received.");
        }

        currentHeader = header;
    }

    public AggregatedMessage aggregate(ReleasableBytesReference content) {
        if (currentHeader == null) {
            content.close();
            throw new IllegalStateException("Received content without header");
        } else {
            contentAggregation.add(content.retain());
            return null;
        }
    }

    public Header cancelAggregation() {
        if (currentHeader == null) {
            throw new IllegalStateException("Aggregation cancelled, but no aggregation had begun");
        } else {
            final Header header = this.currentHeader;
            Releasables.close(contentAggregation);
            contentAggregation.clear();
            currentHeader = null;
            return header;
        }
    }

    public AggregatedMessage finishAggregation() {
        final ReleasableBytesReference[] references = contentAggregation.toArray(new ReleasableBytesReference[0]);
        final CompositeBytesReference content = new CompositeBytesReference(references);
        final ReleasableBytesReference releasableContent = new ReleasableBytesReference(content, () -> Releasables.close(references));
        final AggregatedMessage aggregated = new AggregatedMessage(currentHeader, releasableContent);
        contentAggregation.clear();
        currentHeader = null;
        return aggregated;
    }

    public boolean isAggregating() {
        return currentHeader != null;
    }

    @Override
    public void close() {
        Releasables.close(contentAggregation);
        contentAggregation.clear();
        currentHeader = null;
    }
}
