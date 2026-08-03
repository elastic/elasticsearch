/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;

/**
 * Adapts a {@link ChunkedToXContent} to {@link ToXContent}.
 */
record ToXContentWrapper(ChunkedToXContent chunkedToXContent) implements ToXContent {
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Iterator<? extends ToXContent> serialization = chunkedToXContent.toXContentChunked(params);
        while (serialization.hasNext()) {
            serialization.next().toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public boolean isFragment() {
        return chunkedToXContent.isFragment();
    }
}
