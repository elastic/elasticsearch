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
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.FromXContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent;

import java.io.IOException;
import java.io.InputStream;

/**
 * Snapshot metadata file format used before v2.0
 */
public class LegacyBlobStoreFormat<T extends ToXContent> extends BlobStoreFormat<T> {

    /**
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader the prototype object that can deserialize objects with type T
     */
    public LegacyBlobStoreFormat(String blobNameFormat, FromXContentBuilder<T> reader, ParseFieldMatcher parseFieldMatcher) {
        super(blobNameFormat, reader, parseFieldMatcher);
    }

    /**
     * Reads and parses the blob with given name.
     *
     * If required the checksum of the blob will be verified.
     *
     * @param blobContainer blob container
     * @param blobName blob name
     * @return parsed blob object
     * @throws IOException
     */
    public T readBlob(BlobContainer blobContainer, String blobName) throws IOException {
        try (InputStream inputStream = blobContainer.readBlob(blobName)) {
            BytesStreamOutput out = new BytesStreamOutput();
            Streams.copy(inputStream, out);
            return read(out.bytes());
        }
    }
}
