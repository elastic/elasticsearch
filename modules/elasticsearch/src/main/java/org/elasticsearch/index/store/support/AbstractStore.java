/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.store.support;

import org.apache.lucene.store.*;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.lucene.Directories;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractStore extends AbstractIndexShardComponent implements Store {

    static final String CHECKSUMS_PREFIX = "_checksums-";

    protected final IndexStore indexStore;

    private volatile ImmutableMap<String, StoreFileMetaData> filesMetadata = ImmutableMap.of();

    private volatile String[] files = Strings.EMPTY_ARRAY;

    private final Object mutex = new Object();

    private final boolean sync;

    protected AbstractStore(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore) {
        super(shardId, indexSettings);
        this.indexStore = indexStore;
        this.sync = componentSettings.getAsBoolean("sync", true); // TODO we don't really need to fsync when using shared gateway...
    }

    protected Directory wrapDirectory(Directory dir) throws IOException {
        return new StoreDirectory(dir);
    }

    @Override public ImmutableMap<String, StoreFileMetaData> list() throws IOException {
        ImmutableMap.Builder<String, StoreFileMetaData> builder = ImmutableMap.builder();
        for (String name : files) {
            StoreFileMetaData md = metaData(name);
            if (md != null) {
                builder.put(md.name(), md);
            }
        }
        return builder.build();
    }

    public StoreFileMetaData metaData(String name) throws IOException {
        StoreFileMetaData md = filesMetadata.get(name);
        if (md == null) {
            return null;
        }
        // IndexOutput not closed, does not exists
        if (md.lastModified() == -1 || md.length() == -1) {
            return null;
        }
        return md;
    }

    @Override public void deleteContent() throws IOException {
        String[] files = directory().listAll();
        IOException lastException = null;
        for (String file : files) {
            if (file.startsWith(CHECKSUMS_PREFIX)) {
                ((StoreDirectory) directory()).deleteFileChecksum(file);
            } else {
                try {
                    directory().deleteFile(file);
                } catch (FileNotFoundException e) {
                    // ignore
                } catch (IOException e) {
                    lastException = e;
                }
            }
        }
        if (lastException != null) {
            throw lastException;
        }
    }

    @Override public void fullDelete() throws IOException {
        deleteContent();
    }

    @Override public ByteSizeValue estimateSize() throws IOException {
        return Directories.estimateSize(directory());
    }

    public static Map<String, String> readChecksums(Directory dir) throws IOException {
        long lastFound = -1;
        for (String name : dir.listAll()) {
            if (!name.startsWith(CHECKSUMS_PREFIX)) {
                continue;
            }
            long current = Long.parseLong(name.substring(CHECKSUMS_PREFIX.length()));
            if (current > lastFound) {
                lastFound = current;
            }
        }
        if (lastFound == -1) {
            return ImmutableMap.of();
        }
        IndexInput indexInput = dir.openInput(CHECKSUMS_PREFIX + lastFound);
        try {
            indexInput.readInt(); // version
            return indexInput.readStringStringMap();
        } catch (Exception e) {
            // failed to load checksums, ignore and return an empty map
            return new HashMap<String, String>();
        } finally {
            indexInput.close();
        }
    }

    public void writeChecksums() throws IOException {
        writeChecksums((StoreDirectory) directory());
    }

    private void writeChecksums(StoreDirectory dir) throws IOException {
        String checksumName = CHECKSUMS_PREFIX + System.currentTimeMillis();
        ImmutableMap<String, StoreFileMetaData> files = list();
        synchronized (mutex) {
            Map<String, String> checksums = new HashMap<String, String>();
            for (StoreFileMetaData metaData : files.values()) {
                if (metaData.checksum() != null) {
                    checksums.put(metaData.name(), metaData.checksum());
                }
            }
            IndexOutput output = dir.createOutput(checksumName, false);
            output.writeInt(0); // version
            output.writeStringStringMap(checksums);
            output.close();
        }
        for (StoreFileMetaData metaData : files.values()) {
            if (metaData.name().startsWith(CHECKSUMS_PREFIX) && !checksumName.equals(metaData.name())) {
                try {
                    dir.deleteFileChecksum(metaData.name());
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Returns <tt>true</tt> by default.
     */
    @Override public boolean suggestUseCompoundFile() {
        return true;
    }

    @Override public void close() throws IOException {
        directory().close();
    }

    @Override public IndexOutput createOutputWithNoChecksum(String name) throws IOException {
        return ((StoreDirectory) directory()).createOutput(name, false);
    }

    @Override public void writeChecksum(String name, String checksum) throws IOException {
        // update the metadata to include the checksum and write a new checksums file
        synchronized (mutex) {
            StoreFileMetaData metaData = filesMetadata.get(name);
            metaData = new StoreFileMetaData(metaData.name(), metaData.length(), metaData.lastModified(), checksum);
            filesMetadata = MapBuilder.newMapBuilder(filesMetadata).put(name, metaData).immutableMap();
            writeChecksums();
        }
    }

    /**
     * The idea of the store directory is to cache file level meta data, as well as md5 of it
     */
    class StoreDirectory extends Directory implements ForceSyncDirectory {

        private final Directory delegate;

        StoreDirectory(Directory delegate) throws IOException {
            this.delegate = delegate;
            synchronized (mutex) {
                Map<String, String> checksums = readChecksums(delegate);
                MapBuilder<String, StoreFileMetaData> builder = MapBuilder.newMapBuilder();
                for (String file : delegate.listAll()) {
                    // BACKWARD CKS SUPPORT
                    if (file.endsWith(".cks")) { // ignore checksum files here
                        continue;
                    }
                    String checksum = checksums.get(file);

                    // BACKWARD CKS SUPPORT
                    if (checksum == null) {
                        if (delegate.fileExists(file + ".cks")) {
                            IndexInput indexInput = delegate.openInput(file + ".cks");
                            try {
                                if (indexInput.length() > 0) {
                                    byte[] checksumBytes = new byte[(int) indexInput.length()];
                                    indexInput.readBytes(checksumBytes, 0, checksumBytes.length, false);
                                    checksum = Unicode.fromBytes(checksumBytes);
                                }
                            } finally {
                                indexInput.close();
                            }
                        }
                    }
                    builder.put(file, new StoreFileMetaData(file, delegate.fileLength(file), delegate.fileModified(file), checksum));
                }
                filesMetadata = builder.immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
            }
        }

        public Directory delegate() {
            return delegate;
        }

        @Override public String[] listAll() throws IOException {
            return files;
        }

        @Override public boolean fileExists(String name) throws IOException {
            return filesMetadata.containsKey(name);
        }

        @Override public long fileModified(String name) throws IOException {
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData == null) {
                throw new FileNotFoundException(name);
            }
            // not set yet (IndexOutput not closed)
            if (metaData.lastModified() != -1) {
                return metaData.lastModified();
            }
            return delegate.fileModified(name);
        }

        @Override public void touchFile(String name) throws IOException {
            delegate.touchFile(name);
            synchronized (mutex) {
                StoreFileMetaData metaData = filesMetadata.get(name);
                if (metaData != null) {
                    metaData = new StoreFileMetaData(metaData.name(), metaData.length(), delegate.fileModified(name), metaData.checksum());
                    filesMetadata = MapBuilder.newMapBuilder(filesMetadata).put(name, metaData).immutableMap();
                }
            }
        }

        public void deleteFileChecksum(String name) throws IOException {
            delegate.deleteFile(name);
            synchronized (mutex) {
                filesMetadata = MapBuilder.newMapBuilder(filesMetadata).remove(name).immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
            }
        }

        @Override public void deleteFile(String name) throws IOException {
            // we don't allow to delete the checksums files, only using the deleteChecksum method
            if (name.startsWith(CHECKSUMS_PREFIX)) {
                return;
            }
            delegate.deleteFile(name);
            synchronized (mutex) {
                filesMetadata = MapBuilder.newMapBuilder(filesMetadata).remove(name).immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
            }
        }

        @Override public long fileLength(String name) throws IOException {
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData == null) {
                throw new FileNotFoundException(name);
            }
            // not set yet (IndexOutput not closed)
            if (metaData.length() != -1) {
                return metaData.length();
            }
            return delegate.fileLength(name);
        }

        @Override public IndexOutput createOutput(String name) throws IOException {
            return createOutput(name, true);
        }

        public IndexOutput createOutput(String name, boolean computeChecksum) throws IOException {
            IndexOutput out = delegate.createOutput(name);
            synchronized (mutex) {
                StoreFileMetaData metaData = new StoreFileMetaData(name, -1, -1, null);
                filesMetadata = MapBuilder.newMapBuilder(filesMetadata).put(name, metaData).immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
            }
            return new StoreIndexOutput(out, name, computeChecksum);
        }

        @Override public IndexInput openInput(String name) throws IOException {
            return delegate.openInput(name);
        }

        @Override public void close() throws IOException {
            delegate.close();
            synchronized (mutex) {
                filesMetadata = ImmutableMap.of();
                files = Strings.EMPTY_ARRAY;
            }
        }

        @Override public Lock makeLock(String name) {
            return delegate.makeLock(name);
        }

        @Override public IndexInput openInput(String name, int bufferSize) throws IOException {
            return delegate.openInput(name, bufferSize);
        }

        @Override public void clearLock(String name) throws IOException {
            delegate.clearLock(name);
        }

        @Override public void setLockFactory(LockFactory lockFactory) throws IOException {
            delegate.setLockFactory(lockFactory);
        }

        @Override public LockFactory getLockFactory() {
            return delegate.getLockFactory();
        }

        @Override public String getLockID() {
            return delegate.getLockID();
        }

        @Override public void sync(Collection<String> names) throws IOException {
            if (sync) {
                delegate.sync(names);
            }
            for (String name : names) {
                // write the checksums file when we sync on the segments file (committed)
                if (!name.equals("segments.gen") && name.startsWith("segments")) {
                    writeChecksums();
                    break;
                }
            }
        }

        @Override public void sync(String name) throws IOException {
            if (sync) {
                delegate.sync(name);
            }
            // write the checksums file when we sync on the segments file (committed)
            if (!name.equals("segments.gen") && name.startsWith("segments")) {
                writeChecksums();
            }
        }

        @Override public void forceSync(String name) throws IOException {
            delegate.sync(name);
        }
    }

    class StoreIndexOutput extends IndexOutput {

        private final IndexOutput delegate;

        private final String name;

        private final Checksum digest;

        StoreIndexOutput(IndexOutput delegate, String name, boolean computeChecksum) {
            this.delegate = delegate;
            this.name = name;
            if (computeChecksum) {
                if ("segments.gen".equals(name)) {
                    // no need to create checksum for segments.gen since its not snapshot to recovery
                    this.digest = null;
                } else if (name.startsWith("segments")) {
                    // don't compute checksum for segments files, so pure Lucene can open this directory
                    // and since we, in any case, always recover the segments files
                    this.digest = null;
                } else {
//                    this.digest = new CRC32();
                    // adler is faster, and we compare on length as well, should be enough to check for difference
                    // between files
                    this.digest = new Adler32();
                }
            } else {
                this.digest = null;
            }
        }

        @Override public void close() throws IOException {
            delegate.close();
            String checksum = null;
            if (digest != null) {
                checksum = Long.toString(digest.getValue(), Character.MAX_RADIX);
            }
            synchronized (mutex) {
                StoreFileMetaData md = new StoreFileMetaData(name, directory().fileLength(name), directory().fileModified(name), checksum);
                filesMetadata = MapBuilder.newMapBuilder(filesMetadata).put(name, md).immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
            }
        }

        @Override public void writeByte(byte b) throws IOException {
            delegate.writeByte(b);
            if (digest != null) {
                digest.update(b);
            }
        }

        @Override public void writeBytes(byte[] b, int offset, int length) throws IOException {
            delegate.writeBytes(b, offset, length);
            if (digest != null) {
                digest.update(b, offset, length);
            }
        }

        // don't override it, base class method simple reads from input and writes to this output
//        @Override public void copyBytes(IndexInput input, long numBytes) throws IOException {
//            delegate.copyBytes(input, numBytes);
//        }

        @Override public void flush() throws IOException {
            delegate.flush();
        }

        @Override public long getFilePointer() {
            return delegate.getFilePointer();
        }

        @Override public void seek(long pos) throws IOException {
            // seek might be called on files, which means that the checksum is not file checksum
            // but a checksum of the bytes written to this stream, which is the same for each
            // type of file in lucene
            delegate.seek(pos);
        }

        @Override public long length() throws IOException {
            return delegate.length();
        }

        @Override public void setLength(long length) throws IOException {
            delegate.setLength(length);
        }

        @Override public void writeStringStringMap(Map<String, String> map) throws IOException {
            delegate.writeStringStringMap(map);
        }
    }
}
