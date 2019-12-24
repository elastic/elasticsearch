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
package org.elasticsearch.repositories;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public final class RepositoryCleanupResult implements Writeable, ToXContentObject {

    public static final ObjectParser<RepositoryCleanupResult, Void> PARSER =
        new ObjectParser<>(RepositoryCleanupResult.class.getName(), true, RepositoryCleanupResult::new);

    private static final String DELETED_BLOBS = "deleted_blobs";

    private static final String DELETED_BYTES = "deleted_bytes";

    static {
        PARSER.declareLong((result, bytes) -> result.bytes = bytes, new ParseField(DELETED_BYTES));
        PARSER.declareLong((result, blobs) -> result.blobs = blobs, new ParseField(DELETED_BLOBS));
    }

    private long bytes;

    private long blobs;

    private RepositoryCleanupResult() {
        this(DeleteResult.ZERO);
    }

    public RepositoryCleanupResult(DeleteResult result) {
        this.blobs = result.blobsDeleted();
        this.bytes = result.bytesDeleted();
    }

    public RepositoryCleanupResult(StreamInput in) throws IOException {
        bytes = in.readLong();
        blobs = in.readLong();
    }

    public long bytes() {
        return bytes;
    }

    public long blobs() {
        return blobs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(bytes);
        out.writeLong(blobs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(DELETED_BYTES, bytes).field(DELETED_BLOBS, blobs).endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
