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

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.FromXContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Base class that handles serialization of various data structures during snapshot/restore operations.
 */
public abstract class BlobStoreFormat<T extends ToXContent> {

    protected final String blobNameFormat;

    protected final FromXContentBuilder<T> reader;

    protected final ParseFieldMatcher parseFieldMatcher;

    // Serialization parameters to specify correct context for metadata serialization
    protected static final ToXContent.Params SNAPSHOT_ONLY_FORMAT_PARAMS;

    static {
        Map<String, String> snapshotOnlyParams = new HashMap<>();
        // when metadata is serialized certain elements of the metadata shouldn't be included into snapshot
        // exclusion of these elements is done by setting MetaData.CONTEXT_MODE_PARAM to MetaData.CONTEXT_MODE_SNAPSHOT
        snapshotOnlyParams.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_SNAPSHOT);
        SNAPSHOT_ONLY_FORMAT_PARAMS = new ToXContent.MapParams(snapshotOnlyParams);
    }

    /**
     * @param blobNameFormat format of the blobname in {@link String#format(Locale, String, Object...)} format
     * @param reader the prototype object that can deserialize objects with type T
     * @param parseFieldMatcher parse field matcher
     */
    protected BlobStoreFormat(String blobNameFormat, FromXContentBuilder<T> reader, ParseFieldMatcher parseFieldMatcher) {
        this.reader = reader;
        this.blobNameFormat = blobNameFormat;
        this.parseFieldMatcher = parseFieldMatcher;
    }

    /**
     * Reads and parses the blob with given blob name.
     *
     * @param blobContainer blob container
     * @param blobName blob name
     * @return parsed blob object
     * @throws IOException
     */
    public abstract T readBlob(BlobContainer blobContainer, String blobName) throws IOException;

    /**
     * Reads and parses the blob with given name, applying name translation using the {link #blobName} method
     *
     * @param blobContainer blob container
     * @param name          name to be translated into
     * @return parsed blob object
     * @throws IOException
     */
    public T read(BlobContainer blobContainer, String name) throws IOException {
        String blobName = blobName(name);
        return readBlob(blobContainer, blobName);
    }


    /**
     * Deletes obj in the blob container
     */
    public void delete(BlobContainer blobContainer, String name) throws IOException {
        blobContainer.deleteBlob(blobName(name));
    }

    /**
     * Checks obj in the blob container
     */
    public boolean exists(BlobContainer blobContainer, String name) throws IOException {
        return blobContainer.blobExists(blobName(name));
    }

    protected String blobName(String name) {
        return String.format(Locale.ROOT, blobNameFormat, name);
    }

    protected T read(BytesReference bytes) throws IOException {
        try (XContentParser parser = XContentHelper.createParser(bytes)) {
            T obj = reader.fromXContent(parser, parseFieldMatcher);
            return obj;

        }
    }
}
