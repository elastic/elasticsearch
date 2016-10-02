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
package org.elasticsearch.common.blobstore;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.ESBlobStoreTestCase;

import java.io.IOException;
import java.nio.file.Path;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class FsBlobStoreTests extends ESBlobStoreTestCase {
    protected BlobStore newBlobStore() throws IOException {
        Path tempDir = createTempDir();
        Settings settings = randomBoolean() ? Settings.EMPTY : Settings.builder().put("buffer_size", new ByteSizeValue(randomIntBetween(1, 100), ByteSizeUnit.KB)).build();
        return new FsBlobStore(settings, tempDir);
    }
}
