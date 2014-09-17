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
package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 */
public class FileInfoTest extends ElasticsearchTestCase {

    @Test
    public void testToFromXContent() throws IOException {
        final int iters = scaledRandomIntBetween(1, 10);
        for (int iter = 0; iter < iters; iter++) {
            final BytesRef hash = new BytesRef(scaledRandomIntBetween(0, 1024 * 1024));
            hash.length = hash.bytes.length;
            for (int i = 0; i < hash.length; i++) {
                hash.bytes[i] = randomByte();
            }
            StoreFileMetaData meta = new StoreFileMetaData("foobar", randomInt(), randomAsciiOfLengthBetween(1, 10), TEST_VERSION_CURRENT, hash);
            ByteSizeValue size = new ByteSizeValue(Math.max(0,Math.abs(randomLong())));
            BlobStoreIndexShardSnapshot.FileInfo info = new BlobStoreIndexShardSnapshot.FileInfo("_foobar", meta, size);
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            BlobStoreIndexShardSnapshot.FileInfo.toXContent(info, builder, ToXContent.EMPTY_PARAMS);
            byte[] xcontent = builder.bytes().toBytes();

            final BlobStoreIndexShardSnapshot.FileInfo parsedInfo;
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(xcontent)) {
                parser.nextToken();
                parsedInfo = BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
            }
            assertThat(info.name(), equalTo(parsedInfo.name()));
            assertThat(info.physicalName(), equalTo(parsedInfo.physicalName()));
            assertThat(info.length(), equalTo(parsedInfo.length()));
            assertThat(info.checksum(), equalTo(parsedInfo.checksum()));
            assertThat(info.partBytes(), equalTo(parsedInfo.partBytes()));
            assertThat(parsedInfo.metadata().hash().length, equalTo(hash.length));
            assertThat(parsedInfo.metadata().hash(), equalTo(hash));
            assertThat(parsedInfo.metadata().writtenBy(), equalTo(TEST_VERSION_CURRENT));
            assertThat(parsedInfo.isSame(info.metadata()), is(true));
        }
    }
}
