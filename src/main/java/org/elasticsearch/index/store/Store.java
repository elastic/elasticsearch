/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.lucene.store.*;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Directories;
import org.elasticsearch.common.lucene.store.BufferedChecksumIndexOutput;
import org.elasticsearch.common.lucene.store.ChecksumIndexOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.CloseableIndexComponent;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.distributor.Distributor;
import org.elasticsearch.index.store.support.ForceSyncDirectory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Adler32;

/**
 */
public class Store extends AbstractIndexShardComponent implements CloseableIndexComponent {

    static final String CHECKSUMS_PREFIX = "_checksums-";

    public static final boolean isChecksum(String name) {
        return name.startsWith(CHECKSUMS_PREFIX);
    }

    private final IndexStore indexStore;
    private final DirectoryService directoryService;
    private final StoreDirectory directory;

    private volatile ImmutableOpenMap<String, StoreFileMetaData> filesMetadata = ImmutableOpenMap.of();
    private volatile String[] files = Strings.EMPTY_ARRAY;
    private final Object mutex = new Object();

    private final boolean sync;

    @Inject
    public Store(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore, DirectoryService directoryService, Distributor distributor) throws IOException {
        super(shardId, indexSettings);
        this.indexStore = indexStore;
        this.directoryService = directoryService;
        this.sync = componentSettings.getAsBoolean("sync", true); // TODO we don't really need to fsync when using shared gateway...
        this.directory = new StoreDirectory(distributor);
    }

    public IndexStore indexStore() {
        return this.indexStore;
    }

    public Directory directory() {
        return directory;
    }

    public ImmutableMap<String, StoreFileMetaData> list() throws IOException {
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
        if (md.length() == -1) {
            return null;
        }
        return md;
    }

    /**
     * Deletes the content of a shard store. Be careful calling this!.
     */
    public void deleteContent() throws IOException {
        String[] files = directory.listAll();
        IOException lastException = null;
        for (String file : files) {
            if (isChecksum(file)) {
                try {
                    directory.deleteFileChecksum(file);
                } catch (IOException e) {
                    lastException = e;
                }
            } else {
                try {
                    directory.deleteFile(file);
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

    public StoreStats stats() throws IOException {
        return new StoreStats(Directories.estimateSize(directory), directoryService.throttleTimeInNanos());
    }

    public ByteSizeValue estimateSize() throws IOException {
        return new ByteSizeValue(Directories.estimateSize(directory));
    }

    public void renameFile(String from, String to) throws IOException {
        synchronized (mutex) {
            StoreFileMetaData fromMetaData = filesMetadata.get(from); // we should always find this one
            if (fromMetaData == null) {
                throw new FileNotFoundException(from);
            }
            directoryService.renameFile(fromMetaData.directory(), from, to);
            StoreFileMetaData toMetaData = new StoreFileMetaData(to, fromMetaData.length(), fromMetaData.checksum(), fromMetaData.directory());
            filesMetadata = ImmutableOpenMap.builder(filesMetadata).fRemove(from).fPut(to, toMetaData).build();
            files = filesMetadata.keys().toArray(String.class);
        }
    }

    public static Map<String, String> readChecksums(File[] locations) throws IOException {
        Directory[] dirs = new Directory[locations.length];
        try {
            for (int i = 0; i < locations.length; i++) {
                dirs[i] = new SimpleFSDirectory(locations[i]);
            }
            return readChecksums(dirs, null);
        } finally {
            for (Directory dir : dirs) {
                if (dir != null) {
                    try {
                        dir.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    static Map<String, String> readChecksums(Directory[] dirs, Map<String, String> defaultValue) throws IOException {
        long lastFound = -1;
        Directory lastDir = null;
        for (Directory dir : dirs) {
            for (String name : dir.listAll()) {
                if (!isChecksum(name)) {
                    continue;
                }
                long current = Long.parseLong(name.substring(CHECKSUMS_PREFIX.length()));
                if (current > lastFound) {
                    lastFound = current;
                    lastDir = dir;
                }
            }
        }
        if (lastFound == -1) {
            return defaultValue;
        }
        IndexInput indexInput = lastDir.openInput(CHECKSUMS_PREFIX + lastFound, IOContext.READONCE);
        try {
            indexInput.readInt(); // version
            return indexInput.readStringStringMap();
        } catch (Throwable e) {
            // failed to load checksums, ignore and return an empty map
            return defaultValue;
        } finally {
            indexInput.close();
        }
    }

    public void writeChecksums() throws IOException {
        ImmutableMap<String, StoreFileMetaData> files = list();
        String checksumName = CHECKSUMS_PREFIX + System.currentTimeMillis();
        synchronized (mutex) {
            Map<String, String> checksums = new HashMap<String, String>();
            for (StoreFileMetaData metaData : files.values()) {
                if (metaData.checksum() != null) {
                    checksums.put(metaData.name(), metaData.checksum());
                }
            }
            while (directory.fileExists(checksumName)) {
                checksumName = CHECKSUMS_PREFIX + System.currentTimeMillis();
            }
            IndexOutput output = directory.createOutput(checksumName, IOContext.DEFAULT, true);
            try {
                output.writeInt(0); // version
                output.writeStringStringMap(checksums);
            } finally {
                output.close();
            }

        }
        for (StoreFileMetaData metaData : files.values()) {
            if (metaData.name().startsWith(CHECKSUMS_PREFIX) && !checksumName.equals(metaData.name())) {
                try {
                    directory.deleteFileChecksum(metaData.name());
                } catch (Throwable e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Returns <tt>true</tt> by default.
     */
    public boolean suggestUseCompoundFile() {
        return false;
    }

    public void close() {
        try {
            directory.close();
        } catch (IOException e) {
            logger.debug("failed to close directory", e);
        }
    }

    /**
     * Creates a raw output, no checksum is computed, and no compression if enabled.
     */
    public IndexOutput createOutputRaw(String name) throws IOException {
        return directory.createOutput(name, IOContext.DEFAULT, true);
    }

    /**
     * Opened an index input in raw form, no decompression for example.
     */
    public IndexInput openInputRaw(String name, IOContext context) throws IOException {
        StoreFileMetaData metaData = filesMetadata.get(name);
        if (metaData == null) {
            throw new FileNotFoundException(name);
        }
        return metaData.directory().openInput(name, context);
    }

    public void writeChecksum(String name, String checksum) throws IOException {
        // update the metadata to include the checksum and write a new checksums file
        synchronized (mutex) {
            StoreFileMetaData metaData = filesMetadata.get(name);
            metaData = new StoreFileMetaData(metaData.name(), metaData.length(), checksum, metaData.directory());
            filesMetadata = ImmutableOpenMap.builder(filesMetadata).fPut(name, metaData).build();
            writeChecksums();
        }
    }

    public void writeChecksums(Map<String, String> checksums) throws IOException {
        // update the metadata to include the checksum and write a new checksums file
        synchronized (mutex) {
            for (Map.Entry<String, String> entry : checksums.entrySet()) {
                StoreFileMetaData metaData = filesMetadata.get(entry.getKey());
                metaData = new StoreFileMetaData(metaData.name(), metaData.length(), entry.getValue(), metaData.directory());
                filesMetadata = ImmutableOpenMap.builder(filesMetadata).fPut(entry.getKey(), metaData).build();
            }
            writeChecksums();
        }
    }

    /**
     * The idea of the store directory is to cache file level meta data, as well as md5 of it
     */
    public class StoreDirectory extends BaseDirectory implements ForceSyncDirectory {

        private final Distributor distributor;

        StoreDirectory(Distributor distributor) throws IOException {
            this.distributor = distributor;
            synchronized (mutex) {
                ImmutableOpenMap.Builder<String, StoreFileMetaData> builder = ImmutableOpenMap.builder();
                Map<String, String> checksums = readChecksums(distributor.all(), new HashMap<String, String>());
                for (Directory delegate : distributor.all()) {
                    for (String file : delegate.listAll()) {
                        String checksum = checksums.get(file);
                        builder.put(file, new StoreFileMetaData(file, delegate.fileLength(file), checksum, delegate));
                    }
                }
                filesMetadata = builder.build();
                files = filesMetadata.keys().toArray(String.class);
            }
        }

        public ShardId shardId() {
            return Store.this.shardId();
        }

        public Directory[] delegates() {
            return distributor.all();
        }

        @Override
        public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
            ensureOpen();
            // lets the default implementation happen, so we properly open an input and create an output
            super.copy(to, src, dest, context);
        }

        @Override
        public String[] listAll() throws IOException {
            ensureOpen();
            return files;
        }

        @Override
        public boolean fileExists(String name) throws IOException {
            ensureOpen();
            return filesMetadata.containsKey(name);
        }

        public void deleteFileChecksum(String name) throws IOException {
            ensureOpen();
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData != null) {
                try {
                    metaData.directory().deleteFile(name);
                } catch (IOException e) {
                    if (metaData.directory().fileExists(name)) {
                        throw e;
                    }
                }
            }
            synchronized (mutex) {
                filesMetadata = ImmutableOpenMap.builder(filesMetadata).fRemove(name).build();
                files = filesMetadata.keys().toArray(String.class);
            }
        }

        @Override
        public void deleteFile(String name) throws IOException {
            ensureOpen();
            // we don't allow to delete the checksums files, only using the deleteChecksum method
            if (isChecksum(name)) {
                return;
            }
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData != null) {
                try {
                    metaData.directory().deleteFile(name);
                } catch (IOException e) {
                    if (metaData.directory().fileExists(name)) {
                        throw e;
                    }
                }
            }
            synchronized (mutex) {
                filesMetadata = ImmutableOpenMap.builder(filesMetadata).fRemove(name).build();
                files = filesMetadata.keys().toArray(String.class);
            }
        }

        /**
         * Returns the *actual* file length, not the uncompressed one if compression is enabled, this
         * messes things up when using compound file format, but it shouldn't be used in any case...
         */
        @Override
        public long fileLength(String name) throws IOException {
            ensureOpen();
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData == null) {
                throw new FileNotFoundException(name);
            }
            // not set yet (IndexOutput not closed)
            if (metaData.length() != -1) {
                return metaData.length();
            }
            return metaData.directory().fileLength(name);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            return createOutput(name, context, false);
        }

        public IndexOutput createOutput(String name, IOContext context, boolean raw) throws IOException {
            ensureOpen();
            Directory directory;
            if (isChecksum(name)) {
                directory = distributor.primary();
            } else {
                directory = distributor.any();
            }
            IndexOutput out = directory.createOutput(name, context);
            boolean success = false;
            try {
                synchronized (mutex) {
                    StoreFileMetaData metaData = new StoreFileMetaData(name, -1, null, directory);
                    filesMetadata = ImmutableOpenMap.builder(filesMetadata).fPut(name, metaData).build();
                    files = filesMetadata.keys().toArray(String.class);
                    boolean computeChecksum = !raw;
                    if (computeChecksum) {
                        // don't compute checksum for segment based files
                        if ("segments.gen".equals(name) || name.startsWith("segments")) {
                            computeChecksum = false;
                        }
                    }
                    if (computeChecksum) {
                        out = new BufferedChecksumIndexOutput(out, new Adler32());
                    }

                    final StoreIndexOutput storeIndexOutput = new StoreIndexOutput(metaData, out, name);
                    success = true;
                    return storeIndexOutput;
                }
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(out);
                }
            }
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            ensureOpen();
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData == null) {
                throw new FileNotFoundException(name);
            }
            IndexInput in = metaData.directory().openInput(name, context);
            boolean success = false;
            try {
                // Only for backward comp. since we now use Lucene codec compression
                if (name.endsWith(".fdt") || name.endsWith(".tvf")) {
                    Compressor compressor = CompressorFactory.compressor(in);
                    if (compressor != null) {
                        in = compressor.indexInput(in);
                    }
                }
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(in);
                }
            }
            return in;
        }

        @Override
        public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
            ensureOpen();
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData == null) {
                throw new FileNotFoundException(name);
            }
            // Only for backward comp. since we now use Lucene codec compression
            if (name.endsWith(".fdt") || name.endsWith(".tvf")) {
                // rely on the slicer from the base class that uses an input, since they might be compressed...
                // note, it seems like slicers are only used in compound file format..., so not relevant for now
                return super.createSlicer(name, context);
            }
            return metaData.directory().createSlicer(name, context);
        }

        @Override
        public synchronized void close() throws IOException {
            isOpen = false;
            for (Directory delegate : distributor.all()) {
                delegate.close();
            }
            synchronized (mutex) {
                filesMetadata = ImmutableOpenMap.of();
                files = Strings.EMPTY_ARRAY;
            }
        }

        @Override
        public Lock makeLock(String name) {
            return distributor.primary().makeLock(name);
        }

        @Override
        public void clearLock(String name) throws IOException {
            distributor.primary().clearLock(name);
        }

        @Override
        public void setLockFactory(LockFactory lockFactory) throws IOException {
            distributor.primary().setLockFactory(lockFactory);
        }

        @Override
        public LockFactory getLockFactory() {
            return distributor.primary().getLockFactory();
        }

        @Override
        public String getLockID() {
            return distributor.primary().getLockID();
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            ensureOpen();
            if (sync) {
                Map<Directory, Collection<String>> map = Maps.newHashMap();
                for (String name : names) {
                    StoreFileMetaData metaData = filesMetadata.get(name);
                    if (metaData == null) {
                        throw new FileNotFoundException(name);
                    }
                    Collection<String> dirNames = map.get(metaData.directory());
                    if (dirNames == null) {
                        dirNames = new ArrayList<String>();
                        map.put(metaData.directory(), dirNames);
                    }
                    dirNames.add(name);
                }
                for (Map.Entry<Directory, Collection<String>> entry : map.entrySet()) {
                    entry.getKey().sync(entry.getValue());
                }
            }
            for (String name : names) {
                // write the checksums file when we sync on the segments file (committed)
                if (!name.equals("segments.gen") && name.startsWith("segments")) {
                    writeChecksums();
                    break;
                }
            }
        }

        @Override
        public void forceSync(String name) throws IOException {
            sync(ImmutableList.of(name));
        }

        @Override
        public String toString() {
            return "store(" + distributor.toString() + ")";
        }
    }

    class StoreIndexOutput extends IndexOutput {

        private final StoreFileMetaData metaData;

        private final IndexOutput out;

        private final String name;

        StoreIndexOutput(StoreFileMetaData metaData, IndexOutput delegate, String name) {
            this.metaData = metaData;
            this.out = delegate;
            this.name = name;
        }

        @Override
        public void close() throws IOException {
            out.close();
            String checksum = null;
            IndexOutput underlying = out;
            if (underlying instanceof BufferedChecksumIndexOutput) {
                checksum = Long.toString(((BufferedChecksumIndexOutput) underlying).digest().getValue(), Character.MAX_RADIX);
            } else if (underlying instanceof ChecksumIndexOutput) {
                checksum = Long.toString(((ChecksumIndexOutput) underlying).digest().getValue(), Character.MAX_RADIX);
            }
            synchronized (mutex) {
                StoreFileMetaData md = new StoreFileMetaData(name, metaData.directory().fileLength(name), checksum, metaData.directory());
                filesMetadata = ImmutableOpenMap.builder(filesMetadata).fPut(name, md).build();
                files = filesMetadata.keys().toArray(String.class);
            }
        }

        @Override
        public void copyBytes(DataInput input, long numBytes) throws IOException {
            out.copyBytes(input, numBytes);
        }

        @Override
        public long getFilePointer() {
            return out.getFilePointer();
        }

        @Override
        public void writeByte(byte b) throws IOException {
            out.writeByte(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            out.writeBytes(b, offset, length);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void seek(long pos) throws IOException {
            out.seek(pos);
        }

        @Override
        public long length() throws IOException {
            return out.length();
        }

        @Override
        public void setLength(long length) throws IOException {
            out.setLength(length);
        }

        @Override
        public String toString() {
            return out.toString();
        }
    }
}
