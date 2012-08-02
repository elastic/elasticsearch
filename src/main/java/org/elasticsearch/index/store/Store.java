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
import jsr166y.ThreadLocalRandom;
import org.apache.lucene.store.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedIndexOutput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Directories;
import org.elasticsearch.common.lucene.store.BufferedChecksumIndexOutput;
import org.elasticsearch.common.lucene.store.ChecksumIndexOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
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
public class Store extends AbstractIndexShardComponent {

    static {
        IndexMetaData.addDynamicSettings(
                "index.store.compress.stored",
                "index.store.compress.tv"
        );
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            boolean compressStored = settings.getAsBoolean("index.store.compress.stored", Store.this.compressStored);
            if (compressStored != Store.this.compressStored) {
                logger.info("updating [index.store.compress.stored] from [{}] to [{}]", Store.this.compressStored, compressStored);
                Store.this.compressStored = compressStored;
            }
            boolean compressTv = settings.getAsBoolean("index.store.compress.tv", Store.this.compressTv);
            if (compressTv != Store.this.compressTv) {
                logger.info("updating [index.store.compress.tv] from [{}] to [{}]", Store.this.compressTv, compressTv);
                Store.this.compressTv = compressTv;
            }
        }
    }


    static final String CHECKSUMS_PREFIX = "_checksums-";

    public static final boolean isChecksum(String name) {
        return name.startsWith(CHECKSUMS_PREFIX);
    }

    private final IndexStore indexStore;

    private final IndexSettingsService indexSettingsService;

    private final DirectoryService directoryService;

    private final StoreDirectory directory;

    private volatile ImmutableMap<String, StoreFileMetaData> filesMetadata = ImmutableMap.of();

    private volatile String[] files = Strings.EMPTY_ARRAY;

    private final Object mutex = new Object();

    private final boolean sync;

    private volatile boolean compressStored;
    private volatile boolean compressTv;

    private final ApplySettings applySettings = new ApplySettings();


    @Inject
    public Store(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore, IndexSettingsService indexSettingsService, DirectoryService directoryService) throws IOException {
        super(shardId, indexSettings);
        this.indexStore = indexStore;
        this.indexSettingsService = indexSettingsService;
        this.directoryService = directoryService;
        this.sync = componentSettings.getAsBoolean("sync", true); // TODO we don't really need to fsync when using shared gateway...
        this.directory = new StoreDirectory(directoryService.build());

        this.compressStored = componentSettings.getAsBoolean("compress.stored", false);
        this.compressTv = componentSettings.getAsBoolean("compress.tv", false);

        logger.debug("using compress.stored [{}], compress.tv [{}]", compressStored, compressTv);

        indexSettingsService.addListener(applySettings);
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
        if (md.lastModified() == -1 || md.length() == -1) {
            return null;
        }
        return md;
    }

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

    public void fullDelete() throws IOException {
        deleteContent();
        for (Directory delegate : directory.delegates()) {
            directoryService.fullDelete(delegate);
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
            StoreFileMetaData toMetaData = new StoreFileMetaData(to, fromMetaData.length(), fromMetaData.lastModified(), fromMetaData.checksum(), fromMetaData.directory());
            filesMetadata = MapBuilder.newMapBuilder(filesMetadata).remove(from).put(to, toMetaData).immutableMap();
            files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
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
        IndexInput indexInput = lastDir.openInput(CHECKSUMS_PREFIX + lastFound);
        try {
            indexInput.readInt(); // version
            return indexInput.readStringStringMap();
        } catch (Exception e) {
            // failed to load checksums, ignore and return an empty map
            return defaultValue;
        } finally {
            indexInput.close();
        }
    }

    public void writeChecksums() throws IOException {
        String checksumName = CHECKSUMS_PREFIX + System.currentTimeMillis();
        ImmutableMap<String, StoreFileMetaData> files = list();
        synchronized (mutex) {
            Map<String, String> checksums = new HashMap<String, String>();
            for (StoreFileMetaData metaData : files.values()) {
                if (metaData.checksum() != null) {
                    checksums.put(metaData.name(), metaData.checksum());
                }
            }
            IndexOutput output = directory.createOutput(checksumName, true);
            output.writeInt(0); // version
            output.writeStringStringMap(checksums);
            output.close();
        }
        for (StoreFileMetaData metaData : files.values()) {
            if (metaData.name().startsWith(CHECKSUMS_PREFIX) && !checksumName.equals(metaData.name())) {
                try {
                    directory.deleteFileChecksum(metaData.name());
                } catch (Exception e) {
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

    public void close() throws IOException {
        indexSettingsService.removeListener(applySettings);
        directory.close();
    }

    /**
     * Creates a raw output, no checksum is computed, and no compression if enabled.
     */
    public IndexOutput createOutputRaw(String name) throws IOException {
        return directory.createOutput(name, true);
    }

    /**
     * Opened an index input in raw form, no decompression for example.
     */
    public IndexInput openInputRaw(String name) throws IOException {
        StoreFileMetaData metaData = filesMetadata.get(name);
        if (metaData == null) {
            throw new FileNotFoundException(name);
        }
        return metaData.directory().openInput(name);
    }

    public void writeChecksum(String name, String checksum) throws IOException {
        // update the metadata to include the checksum and write a new checksums file
        synchronized (mutex) {
            StoreFileMetaData metaData = filesMetadata.get(name);
            metaData = new StoreFileMetaData(metaData.name(), metaData.length(), metaData.lastModified(), checksum, metaData.directory());
            filesMetadata = MapBuilder.newMapBuilder(filesMetadata).put(name, metaData).immutableMap();
            writeChecksums();
        }
    }

    public void writeChecksums(Map<String, String> checksums) throws IOException {
        // update the metadata to include the checksum and write a new checksums file
        synchronized (mutex) {
            for (Map.Entry<String, String> entry : checksums.entrySet()) {
                StoreFileMetaData metaData = filesMetadata.get(entry.getKey());
                metaData = new StoreFileMetaData(metaData.name(), metaData.length(), metaData.lastModified(), entry.getValue(), metaData.directory());
                filesMetadata = MapBuilder.newMapBuilder(filesMetadata).put(entry.getKey(), metaData).immutableMap();
            }
            writeChecksums();
        }
    }

    /**
     * The idea of the store directory is to cache file level meta data, as well as md5 of it
     */
    class StoreDirectory extends Directory implements ForceSyncDirectory {

        private final Directory[] delegates;

        StoreDirectory(Directory[] delegates) throws IOException {
            this.delegates = delegates;
            synchronized (mutex) {
                MapBuilder<String, StoreFileMetaData> builder = MapBuilder.newMapBuilder();
                Map<String, String> checksums = readChecksums(delegates, new HashMap<String, String>());
                for (Directory delegate : delegates) {
                    for (String file : delegate.listAll()) {
                        String checksum = checksums.get(file);
                        builder.put(file, new StoreFileMetaData(file, delegate.fileLength(file), delegate.fileModified(file), checksum, delegate));
                    }
                }
                filesMetadata = builder.immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
            }
        }

        public Directory[] delegates() {
            return delegates;
        }

        @Override
        public String[] listAll() throws IOException {
            return files;
        }

        @Override
        public boolean fileExists(String name) throws IOException {
            return filesMetadata.containsKey(name);
        }

        @Override
        public long fileModified(String name) throws IOException {
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData == null) {
                throw new FileNotFoundException(name);
            }
            // not set yet (IndexOutput not closed)
            if (metaData.lastModified() != -1) {
                return metaData.lastModified();
            }
            return metaData.directory().fileModified(name);
        }

        @Override
        public void touchFile(String name) throws IOException {
            synchronized (mutex) {
                StoreFileMetaData metaData = filesMetadata.get(name);
                if (metaData != null) {
                    metaData.directory().touchFile(name);
                    metaData = new StoreFileMetaData(metaData.name(), metaData.length(), metaData.directory().fileModified(name), metaData.checksum(), metaData.directory());
                    filesMetadata = MapBuilder.newMapBuilder(filesMetadata).put(name, metaData).immutableMap();
                }
            }
        }

        public void deleteFileChecksum(String name) throws IOException {
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
                filesMetadata = MapBuilder.newMapBuilder(filesMetadata).remove(name).immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
            }
        }

        @Override
        public void deleteFile(String name) throws IOException {
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
                filesMetadata = MapBuilder.newMapBuilder(filesMetadata).remove(name).immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
            }
        }

        /**
         * Returns the *actual* file length, not the uncompressed one if compression is enabled, this
         * messes things up when using compound file format, but it shouldn't be used in any case...
         */
        @Override
        public long fileLength(String name) throws IOException {
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
        public IndexOutput createOutput(String name) throws IOException {
            return createOutput(name, false);
        }

        public IndexOutput createOutput(String name, boolean raw) throws IOException {
            Directory directory = null;
            if (isChecksum(name)) {
                directory = delegates[0];
            } else {
                if (delegates.length == 1) {
                    directory = delegates[0];
                } else {
                    long size = Long.MIN_VALUE;
                    for (Directory delegate : delegates) {
                        if (delegate instanceof FSDirectory) {
                            long currentSize = ((FSDirectory) delegate).getDirectory().getUsableSpace();
                            if (currentSize > size) {
                                size = currentSize;
                                directory = delegate;
                            } else if (currentSize == size && ThreadLocalRandom.current().nextBoolean()) {
                                directory = delegate;
                            } else {
                            }
                        } else {
                            directory = delegate; // really, make sense to have multiple directories for FS
                        }
                    }
                }
            }
            IndexOutput out = directory.createOutput(name);
            synchronized (mutex) {
                StoreFileMetaData metaData = new StoreFileMetaData(name, -1, -1, null, directory);
                filesMetadata = MapBuilder.newMapBuilder(filesMetadata).put(name, metaData).immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
                boolean computeChecksum = !raw;
                if (computeChecksum) {
                    // don't compute checksum for segment based files
                    if ("segments.gen".equals(name) || name.startsWith("segments")) {
                        computeChecksum = false;
                    }
                }
                if (!raw && ((compressStored && name.endsWith(".fdt")) || (compressTv && name.endsWith(".tvf")))) {
                    if (computeChecksum) {
                        // with compression, there is no need for buffering when doing checksums
                        // since we have buffering on the compressed index output
                        out = new ChecksumIndexOutput(out, new Adler32());
                    }
                    out = CompressorFactory.defaultCompressor().indexOutput(out);
                } else {
                    if (computeChecksum) {
                        out = new BufferedChecksumIndexOutput(out, new Adler32());
                    }
                }
                return new StoreIndexOutput(metaData, out, name);
            }
        }

        @Override
        public IndexInput openInput(String name) throws IOException {
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData == null) {
                throw new FileNotFoundException(name);
            }
            IndexInput in = metaData.directory().openInput(name);
            if (name.endsWith(".fdt") || name.endsWith(".tvf")) {
                Compressor compressor = CompressorFactory.compressor(in);
                if (compressor != null) {
                    in = compressor.indexInput(in);
                }
            }
            return in;
        }

        @Override
        public IndexInput openInput(String name, int bufferSize) throws IOException {
            StoreFileMetaData metaData = filesMetadata.get(name);
            if (metaData == null) {
                throw new FileNotFoundException(name);
            }
            IndexInput in = metaData.directory().openInput(name, bufferSize);
            if (name.endsWith(".fdt") || name.endsWith(".tvf")) {
                Compressor compressor = CompressorFactory.compressor(in);
                if (compressor != null) {
                    in = compressor.indexInput(in);
                }
            }
            return in;
        }

        @Override
        public void close() throws IOException {
            for (Directory delegate : delegates) {
                delegate.close();
            }
            synchronized (mutex) {
                filesMetadata = ImmutableMap.of();
                files = Strings.EMPTY_ARRAY;
            }
        }

        @Override
        public Lock makeLock(String name) {
            return delegates[0].makeLock(name);
        }

        @Override
        public void clearLock(String name) throws IOException {
            delegates[0].clearLock(name);
        }

        @Override
        public void setLockFactory(LockFactory lockFactory) throws IOException {
            delegates[0].setLockFactory(lockFactory);
        }

        @Override
        public LockFactory getLockFactory() {
            return delegates[0].getLockFactory();
        }

        @Override
        public String getLockID() {
            return delegates[0].getLockID();
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
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
        public void sync(String name) throws IOException {
            if (sync) {
                sync(ImmutableList.of(name));
            }
            // write the checksums file when we sync on the segments file (committed)
            if (!name.equals("segments.gen") && name.startsWith("segments")) {
                writeChecksums();
            }
        }

        @Override
        public void forceSync(String name) throws IOException {
            sync(ImmutableList.of(name));
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
            if (out instanceof CompressedIndexOutput) {
                underlying = ((CompressedIndexOutput) out).underlying();
            }
            if (underlying instanceof BufferedChecksumIndexOutput) {
                checksum = Long.toString(((BufferedChecksumIndexOutput) underlying).digest().getValue(), Character.MAX_RADIX);
            } else if (underlying instanceof ChecksumIndexOutput) {
                checksum = Long.toString(((ChecksumIndexOutput) underlying).digest().getValue(), Character.MAX_RADIX);
            }
            synchronized (mutex) {
                StoreFileMetaData md = new StoreFileMetaData(name, metaData.directory().fileLength(name), metaData.directory().fileModified(name), checksum, metaData.directory());
                filesMetadata = MapBuilder.newMapBuilder(filesMetadata).put(name, md).immutableMap();
                files = filesMetadata.keySet().toArray(new String[filesMetadata.size()]);
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
