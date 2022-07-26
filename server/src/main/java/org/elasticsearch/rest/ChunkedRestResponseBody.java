/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Streams;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public interface ChunkedRestResponseBody {

    boolean isDone();

    ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException;

    static ChunkedRestResponseBody fromXContent(ChunkedToXContent chunkedToXContent, ToXContent.Params params, RestChannel channel) {
        return new ChunkedRestResponseBody() {

            final RedirectOutputStream out = new RedirectOutputStream();
            private ChunkedToXContent.ChunkedXContentSerialization serialization;

            private boolean done = false;

            @Override
            public boolean isDone() {
                return done;
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
                out.newTarget(chunkStream);
                if (serialization == null) {
                    serialization = chunkedToXContent.toXContentChunked(
                        channel.newBuilder(channel.request().getXContentType(), null, true, Streams.noCloseStream(out)),
                        params
                    );
                }
                XContentBuilder b;
                while ((b = serialization.encodeChunk()) == null) {
                    if (chunkStream.size() > sizeHint) {
                        break;
                    }
                }
                if (b != null) {
                    done = true;
                    b.close();
                }
                out.clearTarget();
                return new ReleasableBytesReference(chunkStream.bytes(), () -> IOUtils.closeWhileHandlingException(chunkStream));
            }
        };
    }
}
