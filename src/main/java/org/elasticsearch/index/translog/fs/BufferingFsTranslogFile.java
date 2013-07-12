package org.elasticsearch.index.translog.fs;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 */
public class BufferingFsTranslogFile implements FsTranslogFile {

    private final long id;
    private final ShardId shardId;
    private final RafReference raf;

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();

    private volatile int operationCounter;

    private long lastPosition;
    private volatile long lastWrittenPosition;

    private volatile long lastSyncPosition = 0;

    private byte[] buffer;
    private int bufferCount;

    public BufferingFsTranslogFile(ShardId shardId, long id, RafReference raf, int bufferSize) throws IOException {
        this.shardId = shardId;
        this.id = id;
        this.raf = raf;
        this.buffer = new byte[bufferSize];
        raf.raf().setLength(0);
    }

    public long id() {
        return this.id;
    }

    public int estimatedNumberOfOperations() {
        return operationCounter;
    }

    public long translogSizeInBytes() {
        return lastWrittenPosition;
    }

    @Override
    public Translog.Location add(byte[] data, int from, int size) throws IOException {
        rwl.writeLock().lock();
        try {
            operationCounter++;
            long position = lastPosition;
            if (size >= buffer.length) {
                flushBuffer();
                // we use the channel to write, since on windows, writing to the RAF might not be reflected
                // when reading through the channel
                raf.channel().write(ByteBuffer.wrap(data, from, size));
                lastWrittenPosition += size;
                lastPosition += size;
                return new Translog.Location(id, position, size);
            }
            if (size > buffer.length - bufferCount) {
                flushBuffer();
            }
            System.arraycopy(data, from, buffer, bufferCount, size);
            bufferCount += size;
            lastPosition += size;
            return new Translog.Location(id, position, size);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    private void flushBuffer() throws IOException {
        if (bufferCount > 0) {
            // we use the channel to write, since on windows, writing to the RAF might not be reflected
            // when reading through the channel
            raf.channel().write(ByteBuffer.wrap(buffer, 0, bufferCount));
            lastWrittenPosition += bufferCount;
            bufferCount = 0;
        }
    }

    @Override
    public byte[] read(Translog.Location location) throws IOException {
        rwl.readLock().lock();
        try {
            if (location.translogLocation >= lastWrittenPosition) {
                byte[] data = new byte[location.size];
                System.arraycopy(buffer, (int) (location.translogLocation - lastWrittenPosition), data, 0, location.size);
                return data;
            }
        } finally {
            rwl.readLock().unlock();
        }
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        raf.channel().read(buffer, location.translogLocation);
        return buffer.array();
    }

    @Override
    public FsChannelSnapshot snapshot() throws TranslogException {
        rwl.writeLock().lock();
        try {
            flushBuffer();
            if (!raf.increaseRefCount()) {
                return null;
            }
            return new FsChannelSnapshot(this.id, raf, lastWrittenPosition, operationCounter);
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to flush", e);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public boolean syncNeeded() {
        return lastPosition != lastSyncPosition;
    }

    @Override
    public void sync() {
        try {
            // check if we really need to sync here...
            long last = lastPosition;
            if (last == lastSyncPosition) {
                return;
            }
            lastSyncPosition = last;
            rwl.writeLock().lock();
            try {
                flushBuffer();
            } finally {
                rwl.writeLock().unlock();
            }
            raf.channel().force(false);
        } catch (Exception e) {
            // ignore
        }
    }

    @Override
    public void close(boolean delete) {
        if (!delete) {
            rwl.writeLock().lock();
            try {
                flushBuffer();
                sync();
            } catch (IOException e) {
                throw new TranslogException(shardId, "failed to close", e);
            } finally {
                rwl.writeLock().unlock();
            }
        }
        raf.decreaseRefCount(delete);
    }

    @Override
    public void reuse(FsTranslogFile other) {
        if (!(other instanceof BufferingFsTranslogFile)) {
            return;
        }
        rwl.writeLock().lock();
        try {
            flushBuffer();
            this.buffer = ((BufferingFsTranslogFile) other).buffer;
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to flush", e);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public void updateBufferSize(int bufferSize) {
        rwl.writeLock().lock();
        try {
            if (this.buffer.length == bufferSize) {
                return;
            }
            flushBuffer();
            this.buffer = new byte[bufferSize];
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to flush", e);
        } finally {
            rwl.writeLock().unlock();
        }
    }
}
