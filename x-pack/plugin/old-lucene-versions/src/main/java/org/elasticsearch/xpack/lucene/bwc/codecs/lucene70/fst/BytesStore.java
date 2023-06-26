/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// TODO: merge with PagedBytes, except PagedBytes doesn't
// let you read while writing which FST needs

class BytesStore extends DataOutput implements Accountable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesStore.class) + RamUsageEstimator
        .shallowSizeOfInstance(ArrayList.class);

    private final List<byte[]> blocks = new ArrayList<>();

    private final int blockSize;
    private final int blockBits;
    private final int blockMask;

    private byte[] current;
    private int nextWrite;

    BytesStore(int blockBits) {
        this.blockBits = blockBits;
        blockSize = 1 << blockBits;
        blockMask = blockSize - 1;
        nextWrite = blockSize;
    }

    /** Pulls bytes from the provided IndexInput. */
    BytesStore(DataInput in, long numBytes, int maxBlockSize) throws IOException {
        int blockSize = 2;
        int blockBits = 1;
        while (blockSize < numBytes && blockSize < maxBlockSize) {
            blockSize *= 2;
            blockBits++;
        }
        this.blockBits = blockBits;
        this.blockSize = blockSize;
        this.blockMask = blockSize - 1;
        long left = numBytes;
        while (left > 0) {
            final int chunk = (int) Math.min(blockSize, left);
            byte[] block = new byte[chunk];
            in.readBytes(block, 0, block.length);
            blocks.add(block);
            left -= chunk;
        }

        // So .getPosition still works
        nextWrite = blocks.get(blocks.size() - 1).length;
    }

    /** Absolute write byte; you must ensure dest is &lt; max position written so far. */
    public void writeByte(long dest, byte b) {
        int blockIndex = (int) (dest >> blockBits);
        byte[] block = blocks.get(blockIndex);
        block[(int) (dest & blockMask)] = b;
    }

    @Override
    public void writeByte(byte b) {
        if (nextWrite == blockSize) {
            current = new byte[blockSize];
            blocks.add(current);
            nextWrite = 0;
        }
        current[nextWrite++] = b;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int len) {
        while (len > 0) {
            int chunk = blockSize - nextWrite;
            if (len <= chunk) {
                assert b != null;
                assert current != null;
                System.arraycopy(b, offset, current, nextWrite, len);
                nextWrite += len;
                break;
            } else {
                if (chunk > 0) {
                    System.arraycopy(b, offset, current, nextWrite, chunk);
                    offset += chunk;
                    len -= chunk;
                }
                current = new byte[blockSize];
                blocks.add(current);
                nextWrite = 0;
            }
        }
    }

    int getBlockBits() {
        return blockBits;
    }

    /**
     * Absolute writeBytes without changing the current position. Note: this cannot "grow" the bytes,
     * so you must only call it on already written parts.
     */
    void writeBytes(long dest, byte[] b, int offset, int len) {
        // System.out.println(" BS.writeBytes dest=" + dest + " offset=" + offset + " len=" + len);
        assert dest + len <= getPosition() : "dest=" + dest + " pos=" + getPosition() + " len=" + len;

        // Note: weird: must go "backwards" because copyBytes
        // calls us with overlapping src/dest. If we
        // go forwards then we overwrite bytes before we can
        // copy them:

        /*
        int blockIndex = dest >> blockBits;
        int upto = dest & blockMask;
        byte[] block = blocks.get(blockIndex);
        while (len > 0) {
          int chunk = blockSize - upto;
          System.out.println("    cycle chunk=" + chunk + " len=" + len);
          if (len <= chunk) {
        System.arraycopy(b, offset, block, upto, len);
        break;
          } else {
        System.arraycopy(b, offset, block, upto, chunk);
        offset += chunk;
        len -= chunk;
        blockIndex++;
        block = blocks.get(blockIndex);
        upto = 0;
          }
        }
        */

        final long end = dest + len;
        int blockIndex = (int) (end >> blockBits);
        int downTo = (int) (end & blockMask);
        if (downTo == 0) {
            blockIndex--;
            downTo = blockSize;
        }
        byte[] block = blocks.get(blockIndex);

        while (len > 0) {
            // System.out.println(" cycle downTo=" + downTo + " len=" + len);
            if (len <= downTo) {
                // System.out.println(" final: offset=" + offset + " len=" + len + " dest=" +
                // (downTo-len));
                System.arraycopy(b, offset, block, downTo - len, len);
                break;
            } else {
                len -= downTo;
                // System.out.println(" partial: offset=" + (offset + len) + " len=" + downTo + "
                // dest=0");
                System.arraycopy(b, offset + len, block, 0, downTo);
                blockIndex--;
                block = blocks.get(blockIndex);
                downTo = blockSize;
            }
        }
    }

    /**
     * Absolute copy bytes self to self, without changing the position. Note: this cannot "grow" the
     * bytes, so must only call it on already written parts.
     */
    public void copyBytes(long src, long dest, int len) {
        // System.out.println("BS.copyBytes src=" + src + " dest=" + dest + " len=" + len);
        assert src < dest;

        // Note: weird: must go "backwards" because copyBytes
        // calls us with overlapping src/dest. If we
        // go forwards then we overwrite bytes before we can
        // copy them:

        /*
        int blockIndex = src >> blockBits;
        int upto = src & blockMask;
        byte[] block = blocks.get(blockIndex);
        while (len > 0) {
          int chunk = blockSize - upto;
          System.out.println("  cycle: chunk=" + chunk + " len=" + len);
          if (len <= chunk) {
        writeBytes(dest, block, upto, len);
        break;
          } else {
        writeBytes(dest, block, upto, chunk);
        blockIndex++;
        block = blocks.get(blockIndex);
        upto = 0;
        len -= chunk;
        dest += chunk;
          }
        }
        */

        long end = src + len;

        int blockIndex = (int) (end >> blockBits);
        int downTo = (int) (end & blockMask);
        if (downTo == 0) {
            blockIndex--;
            downTo = blockSize;
        }
        byte[] block = blocks.get(blockIndex);

        while (len > 0) {
            // System.out.println(" cycle downTo=" + downTo);
            if (len <= downTo) {
                // System.out.println(" finish");
                writeBytes(dest, block, downTo - len, len);
                break;
            } else {
                // System.out.println(" partial");
                len -= downTo;
                writeBytes(dest + len, block, 0, downTo);
                blockIndex--;
                block = blocks.get(blockIndex);
                downTo = blockSize;
            }
        }
    }

    /** Copies bytes from this store to a target byte array. */
    public void copyBytes(long src, byte[] dest, int offset, int len) {
        int blockIndex = (int) (src >> blockBits);
        int upto = (int) (src & blockMask);
        byte[] block = blocks.get(blockIndex);
        while (len > 0) {
            int chunk = blockSize - upto;
            if (len <= chunk) {
                System.arraycopy(block, upto, dest, offset, len);
                break;
            } else {
                System.arraycopy(block, upto, dest, offset, chunk);
                blockIndex++;
                block = blocks.get(blockIndex);
                upto = 0;
                len -= chunk;
                offset += chunk;
            }
        }
    }

    /** Writes an int at the absolute position without changing the current pointer. */
    public void writeInt(long pos, int value) {
        int blockIndex = (int) (pos >> blockBits);
        int upto = (int) (pos & blockMask);
        byte[] block = blocks.get(blockIndex);
        int shift = 24;
        for (int i = 0; i < 4; i++) {
            block[upto++] = (byte) (value >> shift);
            shift -= 8;
            if (upto == blockSize) {
                upto = 0;
                blockIndex++;
                block = blocks.get(blockIndex);
            }
        }
    }

    /** Reverse from srcPos, inclusive, to destPos, inclusive. */
    public void reverse(long srcPos, long destPos) {
        assert srcPos < destPos;
        assert destPos < getPosition();
        // System.out.println("reverse src=" + srcPos + " dest=" + destPos);

        int srcBlockIndex = (int) (srcPos >> blockBits);
        int src = (int) (srcPos & blockMask);
        byte[] srcBlock = blocks.get(srcBlockIndex);

        int destBlockIndex = (int) (destPos >> blockBits);
        int dest = (int) (destPos & blockMask);
        byte[] destBlock = blocks.get(destBlockIndex);
        // System.out.println(" srcBlock=" + srcBlockIndex + " destBlock=" + destBlockIndex);

        int limit = (int) (destPos - srcPos + 1) / 2;
        for (int i = 0; i < limit; i++) {
            // System.out.println(" cycle src=" + src + " dest=" + dest);
            byte b = srcBlock[src];
            srcBlock[src] = destBlock[dest];
            destBlock[dest] = b;
            src++;
            if (src == blockSize) {
                srcBlockIndex++;
                srcBlock = blocks.get(srcBlockIndex);
                // System.out.println(" set destBlock=" + destBlock + " srcBlock=" + srcBlock);
                src = 0;
            }

            dest--;
            if (dest == -1) {
                destBlockIndex--;
                destBlock = blocks.get(destBlockIndex);
                // System.out.println(" set destBlock=" + destBlock + " srcBlock=" + srcBlock);
                dest = blockSize - 1;
            }
        }
    }

    public void skipBytes(int len) {
        while (len > 0) {
            int chunk = blockSize - nextWrite;
            if (len <= chunk) {
                nextWrite += len;
                break;
            } else {
                len -= chunk;
                current = new byte[blockSize];
                blocks.add(current);
                nextWrite = 0;
            }
        }
    }

    public long getPosition() {
        return ((long) blocks.size() - 1) * blockSize + nextWrite;
    }

    /**
     * Pos must be less than the max position written so far! Ie, you cannot "grow" the file with
     * this!
     */
    public void truncate(long newLen) {
        assert newLen <= getPosition();
        assert newLen >= 0;
        int blockIndex = (int) (newLen >> blockBits);
        nextWrite = (int) (newLen & blockMask);
        if (nextWrite == 0) {
            blockIndex--;
            nextWrite = blockSize;
        }
        blocks.subList(blockIndex + 1, blocks.size()).clear();
        if (newLen == 0) {
            current = null;
        } else {
            current = blocks.get(blockIndex);
        }
        assert newLen == getPosition();
    }

    public void finish() {
        if (current != null) {
            byte[] lastBuffer = new byte[nextWrite];
            System.arraycopy(current, 0, lastBuffer, 0, nextWrite);
            blocks.set(blocks.size() - 1, lastBuffer);
            current = null;
        }
    }

    /** Writes all of our bytes to the target {@link DataOutput}. */
    public void writeTo(DataOutput out) throws IOException {
        for (byte[] block : blocks) {
            out.writeBytes(block, 0, block.length);
        }
    }

    public FST.BytesReader getForwardReader() {
        if (blocks.size() == 1) {
            return new ForwardBytesReader(blocks.get(0));
        }
        return new FST.BytesReader() {
            private byte[] current;
            private int nextBuffer;
            private int nextRead = blockSize;

            @Override
            public byte readByte() {
                if (nextRead == blockSize) {
                    current = blocks.get(nextBuffer++);
                    nextRead = 0;
                }
                return current[nextRead++];
            }

            @Override
            public void skipBytes(long count) {
                setPosition(getPosition() + count);
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) {
                while (len > 0) {
                    int chunkLeft = blockSize - nextRead;
                    if (len <= chunkLeft) {
                        System.arraycopy(current, nextRead, b, offset, len);
                        nextRead += len;
                        break;
                    } else {
                        if (chunkLeft > 0) {
                            System.arraycopy(current, nextRead, b, offset, chunkLeft);
                            offset += chunkLeft;
                            len -= chunkLeft;
                        }
                        current = blocks.get(nextBuffer++);
                        nextRead = 0;
                    }
                }
            }

            @Override
            public long getPosition() {
                return ((long) nextBuffer - 1) * blockSize + nextRead;
            }

            @Override
            public void setPosition(long pos) {
                int bufferIndex = (int) (pos >> blockBits);
                if (nextBuffer != bufferIndex + 1) {
                    nextBuffer = bufferIndex + 1;
                    current = blocks.get(bufferIndex);
                }
                nextRead = (int) (pos & blockMask);
                assert getPosition() == pos;
            }

            @Override
            public boolean reversed() {
                return false;
            }
        };
    }

    public FST.BytesReader getReverseReader() {
        return getReverseReader(true);
    }

    FST.BytesReader getReverseReader(boolean allowSingle) {
        if (allowSingle && blocks.size() == 1) {
            return new ReverseBytesReader(blocks.get(0));
        }
        return new FST.BytesReader() {
            private byte[] current = blocks.size() == 0 ? null : blocks.get(0);
            private int nextBuffer = -1;
            private int nextRead = 0;

            @Override
            public byte readByte() {
                if (nextRead == -1) {
                    current = blocks.get(nextBuffer--);
                    nextRead = blockSize - 1;
                }
                return current[nextRead--];
            }

            @Override
            public void skipBytes(long count) {
                setPosition(getPosition() - count);
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) {
                for (int i = 0; i < len; i++) {
                    b[offset + i] = readByte();
                }
            }

            @Override
            public long getPosition() {
                return ((long) nextBuffer + 1) * blockSize + nextRead;
            }

            @Override
            public void setPosition(long pos) {
                // NOTE: a little weird because if you
                // setPosition(0), the next byte you read is
                // bytes[0] ... but I would expect bytes[-1] (ie,
                // EOF)...?
                int bufferIndex = (int) (pos >> blockBits);
                if (nextBuffer != bufferIndex - 1) {
                    nextBuffer = bufferIndex - 1;
                    current = blocks.get(bufferIndex);
                }
                nextRead = (int) (pos & blockMask);
                assert getPosition() == pos : "pos=" + pos + " getPos()=" + getPosition();
            }

            @Override
            public boolean reversed() {
                return true;
            }
        };
    }

    @Override
    public long ramBytesUsed() {
        long size = BASE_RAM_BYTES_USED;
        for (byte[] block : blocks) {
            size += RamUsageEstimator.sizeOf(block);
        }
        return size;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(numBlocks=" + blocks.size() + ")";
    }
}
