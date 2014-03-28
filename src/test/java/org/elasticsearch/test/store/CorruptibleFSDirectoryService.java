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

package org.elasticsearch.test.store;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.TestCluster;

import java.io.IOException;
import java.util.Random;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.cluster;

public class CorruptibleFSDirectoryService extends MockFSDirectoryService {

    private final Random random;

    private volatile String[] fileNameMask;

    @Inject
    public CorruptibleFSDirectoryService(final ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore, final IndicesService service) {
        super(shardId, indexSettings, indexStore, service);
        final long seed = indexSettings.getAsLong(TestCluster.SETTING_INDEX_SEED, 0l);
        random = new Random(seed);
        fileNameMask = null;
    }

    @Override
    public Directory[] build() throws IOException {
        return wrapAllInPlace(super.build());
    }

    private Directory[] wrapAllInPlace(Directory[] directories) {
        for (int i=0; i<directories.length; i++) {
            directories[i] = wrap(directories[i]);
        }
        return directories;
    }

    private Directory wrap(Directory directory) {
        return new CorruptibleDirectoryWrapper(directory);
    }

    private void enableCorruption(String fileNameMask) {
        if (fileNameMask != null) {
            logger.info("enabling corruption [{}]", fileNameMask);
            this.fileNameMask = Strings.splitStringByCommaToArray(fileNameMask);
        } else {
            logger.info("disabling corruption");
            this.fileNameMask = null;
        }
    }

    public static void enableCorruption(String node, String index, String fileNameMask) {
        IndicesService indicesService = cluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexServiceSafe(index);
        for(int shardId : indexService.shardIds()) {
            DirectoryService directoryService = indexService.shardInjectorSafe(shardId).getInstance(DirectoryService.class);
            if (directoryService instanceof CorruptibleFSDirectoryService) {
                CorruptibleFSDirectoryService corruptibleFSDirectoryService = (CorruptibleFSDirectoryService)directoryService;
                corruptibleFSDirectoryService.enableCorruption(fileNameMask);
            }
        }
    }

    public final class CorruptibleDirectoryWrapper extends BaseDirectoryWrapper {

        public CorruptibleDirectoryWrapper(Directory delegate) {
            super(delegate);
        }

        @Override
        public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
            if (Regex.simpleMatch(fileNameMask, name) ) {
                logger.debug("corrupting file [{}]", name);
                return new CorruptibleIndexInput(super.openInput(name, context), logger, random);
            } else {
                logger.trace("not corrupting file [{}]", name);
                return super.openInput(name, context);
            }
        }

    }

    public static final class CorruptibleIndexInput extends IndexInput {
        private final IndexInput delegate;
        private final Random random;
        private final ESLogger logger;

        public CorruptibleIndexInput(IndexInput delegate, ESLogger logger, Random random) {
            super("CorruptibleIndexInput(" + delegate + ")");
            this.delegate = delegate;
            this.random = random;
            this.logger = logger;
        }

        @Override
        public byte readByte() throws IOException {
            final byte b = delegate.readByte();
            return b;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            long prevPos = delegate.getFilePointer();
            delegate.readBytes(b, offset, len);
            if (len > 0 ) {
                int pos = random.nextInt(len);
                b[pos] ^= 42;
                logger.info("Corrupted byte [{}] in file [{}]", prevPos + pos, delegate);
            }
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public long getFilePointer() {
            return delegate.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            delegate.seek(pos);
        }

        @Override
        public long length() {
            return delegate.length();
        }
    }
}
