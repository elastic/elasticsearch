package org.elasticsearch.common.compress;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.store.*;
import org.elasticsearch.index.store.support.ForceSyncDirectory;

import java.io.IOException;
import java.util.Collection;

/**
 */
public class CompressedDirectory extends Directory implements ForceSyncDirectory {

    private final Directory dir;

    private final Compressor compressor;

    private final boolean actualLength;

    private final ImmutableSet<String> compressExtensions;
    private final ImmutableSet<String> decompressExtensions;

    private volatile boolean compress = true;

    public CompressedDirectory(Directory dir, Compressor compressor, boolean actualLength, String... extensions) {
        this(dir, compressor, actualLength, extensions, extensions);
    }

    public CompressedDirectory(Directory dir, Compressor compressor, boolean actualLength, String[] compressExtensions, String[] decompressExtensions) {
        this.dir = dir;
        this.actualLength = actualLength;
        this.compressor = compressor;
        this.compressExtensions = ImmutableSet.copyOf(compressExtensions);
        this.decompressExtensions = ImmutableSet.copyOf(decompressExtensions);
        this.lockFactory = dir.getLockFactory();
    }

    @Override
    public String[] listAll() throws IOException {
        return dir.listAll();
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    /**
     * Utility method to return a file's extension.
     */
    public static String getExtension(String name) {
        int i = name.lastIndexOf('.');
        if (i == -1) {
            return "";
        }
        return name.substring(i + 1, name.length());
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return dir.fileExists(name);
    }

    @Override
    public long fileModified(String name) throws IOException {
        return dir.fileModified(name);
    }

    @Override
    public void touchFile(String name) throws IOException {
        dir.touchFile(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        dir.deleteFile(name);
    }

    /**
     * Returns the actual file size, so will work with compound file format
     * when compressed. Its the only one that really uses it for offsets...
     */
    @Override
    public long fileLength(String name) throws IOException {
        if (actualLength && decompressExtensions.contains(getExtension(name))) {
            IndexInput in = openInput(name);
            try {
                return in.length();
            } catch (Exception e) {
                in.close();
            }
        }
        return dir.fileLength(name);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        dir.sync(names);
    }

    @Override
    public void sync(String name) throws IOException {
        dir.sync(name);
    }

    @Override
    public void forceSync(String name) throws IOException {
        if (dir instanceof ForceSyncDirectory) {
            ((ForceSyncDirectory) dir).forceSync(name);
        } else {
            dir.sync(name);
        }
    }

    @Override
    public IndexInput openInput(String name) throws IOException {
        if (decompressExtensions.contains(getExtension(name))) {
            IndexInput in = dir.openInput(name);
            Compressor compressor1 = CompressorFactory.compressor(in);
            if (compressor1 != null) {
                return compressor1.indexInput(in);
            } else {
                return in;
            }
        }
        return dir.openInput(name);
    }

    @Override
    public IndexInput openInput(String name, int bufferSize) throws IOException {
        if (decompressExtensions.contains(getExtension(name))) {
            IndexInput in = dir.openInput(name, bufferSize);
            Compressor compressor1 = CompressorFactory.compressor(in);
            if (compressor1 != null) {
                return compressor1.indexInput(in);
            } else {
                return in;
            }
        }
        return dir.openInput(name, bufferSize);
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        if (compress && compressExtensions.contains(getExtension(name))) {
            return compressor.indexOutput(dir.createOutput(name));
        }
        return dir.createOutput(name);
    }

    // can't override this one, we need to open the correct compression
//    @Override
//    public void copy(Directory to, String src, String dest) throws IOException {
//        dir.copy(to, src, dest);
//    }

    @Override
    public void close() throws IOException {
        dir.close();
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        dir.setLockFactory(lockFactory);
    }

    @Override
    public LockFactory getLockFactory() {
        return dir.getLockFactory();
    }

    @Override
    public String getLockID() {
        return dir.getLockID();
    }

    @Override
    public Lock makeLock(String name) {
        return dir.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        dir.clearLock(name);
    }

    @Override
    public String toString() {
        return "compressed(" + compressExtensions + "):" + dir.toString();
    }
}
