/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io;

import org.elasticsearch.core.SuppressForbidden;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

@SuppressForbidden(reason = "Channel#read")
public final class Channels {

    private Channels() {}

    /**
     * The maximum chunk size for reads in bytes
     */
    private static final int READ_CHUNK_SIZE = 16384;
    /**
     * The maximum chunk size for writes in bytes
     */
    public static final int WRITE_CHUNK_SIZE = 8192;

    /**
     * read <i>length</i> bytes from <i>position</i> of a file channel
     */
    public static byte[] readFromFileChannel(FileChannel channel, long position, int length) throws IOException {
        byte[] res = new byte[length];
        readFromFileChannelWithEofException(channel, position, res, 0, length);
        return res;

    }

    /**
     * read <i>length</i> bytes from <i>position</i> of a file channel. An EOFException will be thrown if you
     * attempt to read beyond the end of file.
     *
     * @param channel         channel to read from
     * @param channelPosition position to read from
     * @param dest            destination byte array to put data in
     * @param destOffset      offset in dest to read into
     * @param length          number of bytes to read
     */
    public static void readFromFileChannelWithEofException(
        FileChannel channel,
        long channelPosition,
        byte[] dest,
        int destOffset,
        int length
    ) throws IOException {
        int read = readFromFileChannel(channel, channelPosition, dest, destOffset, length);
        if (read < 0) {
            throw new EOFException("read past EOF. pos [" + channelPosition + "] length: [" + length + "] end: [" + channel.size() + "]");
        }
    }

    /**
     * read <i>length</i> bytes from <i>position</i> of a file channel.
     *
     * @param channel         channel to read from
     * @param channelPosition position to read from
     * @param dest            destination byte array to put data in
     * @param destOffset      offset in dest to read into
     * @param length          number of bytes to read
     * @return total bytes read or -1 if an attempt was made to read past EOF. The method always tries to read all the bytes
     * that will fit in the destination byte buffer.
     */
    public static int readFromFileChannel(FileChannel channel, long channelPosition, byte[] dest, int destOffset, int length)
        throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(dest, destOffset, length);
        return readFromFileChannel(channel, channelPosition, buffer);
    }

    /**
     * read from a file channel into a byte buffer, starting at a certain position.  An EOFException will be thrown if you
     * attempt to read beyond the end of file.
     *
     * @param channel         channel to read from
     * @param channelPosition position to read from
     * @param dest            destination {@link java.nio.ByteBuffer} to put data in
     * @return total bytes read
     */
    public static int readFromFileChannelWithEofException(FileChannel channel, long channelPosition, ByteBuffer dest) throws IOException {
        int read = readFromFileChannel(channel, channelPosition, dest);
        if (read < 0) {
            throw new EOFException(
                "read past EOF. pos [" + channelPosition + "] length: [" + dest.limit() + "] end: [" + channel.size() + "]"
            );
        }
        return read;
    }

    /**
     * read from a file channel into a byte buffer, starting at a certain position.
     *
     * @param channel         channel to read from
     * @param channelPosition position to read from
     * @param dest            destination {@link java.nio.ByteBuffer} to put data in
     * @return total bytes read or -1 if an attempt was made to read past EOF. The method always tries to read all the bytes
     * that will fit in the destination byte buffer.
     */
    public static int readFromFileChannel(FileChannel channel, long channelPosition, ByteBuffer dest) throws IOException {
        if (dest.isDirect() || (dest.remaining() < READ_CHUNK_SIZE)) {
            return readSingleChunk(channel, channelPosition, dest);
        } else {
            int bytesRead = 0;
            int bytesToRead = dest.remaining();

            // duplicate the buffer in order to be able to change the limit
            ByteBuffer tmpBuffer = dest.duplicate();
            try {
                while (dest.hasRemaining()) {
                    tmpBuffer.limit(Math.min(dest.limit(), tmpBuffer.position() + READ_CHUNK_SIZE));
                    int read = readSingleChunk(channel, channelPosition, tmpBuffer);
                    if (read < 0) {
                        return read;
                    }
                    bytesRead += read;
                    channelPosition += read;
                    dest.position(tmpBuffer.position());
                }
            } finally {
                // make sure we update byteBuffer to indicate how far we came..
                dest.position(tmpBuffer.position());
            }

            assert bytesRead == bytesToRead
                : "failed to read an entire buffer but also didn't get an EOF (read [" + bytesRead + "] needed [" + bytesToRead + "]";
            return bytesRead;
        }
    }

    private static int readSingleChunk(FileChannel channel, long channelPosition, ByteBuffer dest) throws IOException {
        int bytesRead = 0;
        while (dest.hasRemaining()) {
            int read = channel.read(dest, channelPosition);
            if (read < 0) {
                return read;
            }

            assert read > 0
                : "FileChannel.read with non zero-length bb.remaining() must always read at least one byte "
                    + "(FileChannel is in blocking mode, see spec of ReadableByteChannel)";

            bytesRead += read;
            channelPosition += read;
        }
        return bytesRead;
    }

    /**
     * Writes part of a byte array to a {@link java.nio.channels.WritableByteChannel}
     *
     * @param source  byte array to copy from
     * @param channel target WritableByteChannel
     */
    public static void writeToChannel(byte[] source, WritableByteChannel channel) throws IOException {
        writeToChannel(source, 0, source.length, channel);
    }

    /**
     * Writes part of a byte array to a {@link java.nio.channels.WritableByteChannel}
     *
     * @param source  byte array to copy from
     * @param offset  start copying from this offset
     * @param length  how many bytes to copy
     * @param channel target WritableByteChannel
     */
    public static void writeToChannel(byte[] source, int offset, int length, WritableByteChannel channel) throws IOException {
        int toWrite = Math.min(length, WRITE_CHUNK_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(source, offset, toWrite);
        int written = channel.write(buffer);
        length -= written;
        while (length > 0) {
            toWrite = Math.min(length, WRITE_CHUNK_SIZE);
            buffer.limit(buffer.position() + toWrite);
            written = channel.write(buffer);
            length -= written;
        }
        assert length == 0 : "wrote more then expected bytes (length=" + length + ")";
    }

    /**
     * Writes part of a byte array to a {@link java.nio.channels.WritableByteChannel} at the provided
     * position.
     *
     * @param source          byte array to copy from
     * @param channel         target WritableByteChannel
     * @param channelPosition position to write at
     */
    public static void writeToChannel(byte[] source, FileChannel channel, long channelPosition) throws IOException {
        writeToChannel(source, 0, source.length, channel, channelPosition);
    }

    /**
     * Writes part of a byte array to a {@link java.nio.channels.WritableByteChannel} at the provided
     * position.
     *
     * @param source          byte array to copy from
     * @param offset          start copying from this offset
     * @param length          how many bytes to copy
     * @param channel         target WritableByteChannel
     * @param channelPosition position to write at
     */
    public static void writeToChannel(byte[] source, int offset, int length, FileChannel channel, long channelPosition) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(source, offset, length);
        int written = channel.write(buffer, channelPosition);
        length -= written;
        while (length > 0) {
            written = channel.write(buffer, channelPosition + buffer.position());
            length -= written;
        }
        assert length == 0 : "wrote more then expected bytes (length=" + length + ")";
    }

    /**
     * Writes a {@link java.nio.ByteBuffer} to a {@link java.nio.channels.WritableByteChannel}
     *
     * @param byteBuffer source buffer
     * @param channel    channel to write to
     */
    public static void writeToChannel(ByteBuffer byteBuffer, WritableByteChannel channel) throws IOException {
        if (byteBuffer.isDirect() || (byteBuffer.remaining() <= WRITE_CHUNK_SIZE)) {
            while (byteBuffer.hasRemaining()) {
                channel.write(byteBuffer);
            }
        } else {
            // duplicate the buffer in order to be able to change the limit
            ByteBuffer tmpBuffer = byteBuffer.duplicate();
            try {
                while (byteBuffer.hasRemaining()) {
                    tmpBuffer.limit(Math.min(byteBuffer.limit(), tmpBuffer.position() + WRITE_CHUNK_SIZE));
                    while (tmpBuffer.hasRemaining()) {
                        channel.write(tmpBuffer);
                    }
                    byteBuffer.position(tmpBuffer.position());
                }
            } finally {
                // make sure we update byteBuffer to indicate how far we came..
                byteBuffer.position(tmpBuffer.position());
            }
        }
    }
}
