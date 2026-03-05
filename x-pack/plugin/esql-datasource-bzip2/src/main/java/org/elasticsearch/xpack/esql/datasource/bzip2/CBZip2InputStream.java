/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * Adapted from Apache Hadoop's CBZip2InputStream (Apache License 2.0).
 * Original: github.com/apache/hadoop - hadoop-common - bzip2/CBZip2InputStream.java
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CBZip2InputStream extends InputStream implements BZip2Constants {

    public enum ReadMode {
        CONTINUOUS,
        BYBLOCK
    }

    public static final long BLOCK_DELIMITER = 0X314159265359L;
    public static final long EOS_DELIMITER = 0X177245385090L;
    private static final int DELIMITER_BIT_LENGTH = 48;

    ReadMode readMode = ReadMode.CONTINUOUS;
    private long reportedBytesReadFromCompressedStream = 0L;
    private long bytesReadFromCompressedStream = 0L;
    private boolean lazyInitialization = false;
    private byte[] array = new byte[1];

    private int last;
    private int origPtr;
    private int blockSize100k;
    private boolean blockRandomised = false;
    private long bsBuff;
    private long bsLive;
    private final BZip2CRC crc = new BZip2CRC();
    private int nInUse;
    private BufferedInputStream in;
    private int currentChar = -1;

    public enum STATE {
        EOF,
        START_BLOCK_STATE,
        RAND_PART_A_STATE,
        RAND_PART_B_STATE,
        RAND_PART_C_STATE,
        NO_RAND_PART_A_STATE,
        NO_RAND_PART_B_STATE,
        NO_RAND_PART_C_STATE,
        NO_PROCESS_STATE
    }

    private STATE currentState = STATE.START_BLOCK_STATE;
    private int storedBlockCRC;
    private int storedCombinedCRC;
    private int computedBlockCRC;
    private int computedCombinedCRC;
    private boolean skipResult = false;
    private boolean skipDecompression = false;

    private int su_count;
    private int su_ch2;
    private int su_chPrev;
    private int su_i2;
    private int su_j2;
    private int su_rNToGo;
    private int su_rTPos;
    private int su_tPos;
    private char su_z;

    private CBZip2InputStream.Data data;

    public long getProcessedByteCount() {
        return reportedBytesReadFromCompressedStream;
    }

    protected void updateProcessedByteCount(int count) {
        this.bytesReadFromCompressedStream += count;
    }

    public void updateReportedByteCount(int count) {
        this.reportedBytesReadFromCompressedStream += count;
        this.updateProcessedByteCount(count);
    }

    private int readAByte(InputStream inStream) throws IOException {
        int read = inStream.read();
        if (read >= 0) {
            this.updateProcessedByteCount(1);
        }
        return read;
    }

    public boolean skipToNextMarker(long marker, int markerBitLength) throws IOException, IllegalArgumentException {
        try {
            if (markerBitLength > 63) {
                throw new IllegalArgumentException("skipToNextMarker can not find patterns greater than 63 bits");
            }
            long bytes = this.bsR(markerBitLength);
            if (bytes == -1) {
                this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream;
                return false;
            }
            while (true) {
                if (bytes == marker) {
                    long markerBytesRead = (markerBitLength + this.bsLive + 7) / 8;
                    this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream - markerBytesRead;
                    return true;
                } else {
                    bytes = bytes << 1;
                    bytes = bytes & ((1L << markerBitLength) - 1);
                    int oneBit = (int) this.bsR(1);
                    if (oneBit != -1) {
                        bytes = bytes | oneBit;
                    } else {
                        this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream;
                        return false;
                    }
                }
            }
        } catch (IOException ex) {
            this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream;
            return false;
        }
    }

    protected void reportCRCError() throws IOException {
        throw new IOException("crc error");
    }

    private void makeMaps() {
        final boolean[] inUse = this.data.inUse;
        final byte[] seqToUnseq = this.data.seqToUnseq;
        int nInUseShadow = 0;

        for (int i = 0; i < 256; i++) {
            if (inUse[i]) {
                seqToUnseq[nInUseShadow++] = (byte) i;
            }
        }

        this.nInUse = nInUseShadow;
    }

    public CBZip2InputStream(final InputStream in, ReadMode readMode) throws IOException {
        this(in, readMode, false);
    }

    @SuppressWarnings("this-escape")
    private CBZip2InputStream(final InputStream in, ReadMode readMode, boolean skipDecompression) throws IOException {
        super();
        int blockSize = 0X39;
        this.blockSize100k = blockSize - '0';
        this.in = new BufferedInputStream(in, 1024 * 9);
        this.readMode = readMode;
        this.skipDecompression = skipDecompression;
        if (readMode == ReadMode.CONTINUOUS) {
            currentState = STATE.START_BLOCK_STATE;
            lazyInitialization = (in.available() == 0);
            if (lazyInitialization == false) {
                init();
            }
        } else if (readMode == ReadMode.BYBLOCK) {
            this.currentState = STATE.NO_PROCESS_STATE;
            skipResult = skipToNextBlockMarker();
            if (skipDecompression == false) {
                changeStateToProcessABlock();
            }
        }
    }

    boolean skipToNextBlockMarker() throws IOException {
        return skipToNextMarker(CBZip2InputStream.BLOCK_DELIMITER, DELIMITER_BIT_LENGTH);
    }

    public static long numberOfBytesTillNextMarker(final InputStream in) throws IOException {
        CBZip2InputStream anObject = new CBZip2InputStream(in, ReadMode.BYBLOCK, true);
        return anObject.getProcessedByteCount();
    }

    public CBZip2InputStream(final InputStream in) throws IOException {
        this(in, ReadMode.CONTINUOUS);
    }

    private void changeStateToProcessABlock() throws IOException {
        if (skipResult) {
            initBlock();
            setupBlock();
        } else {
            this.currentState = STATE.EOF;
        }
    }

    @Override
    public int read() throws IOException {
        if (this.in != null) {
            int result = this.read(array, 0, 1);
            int value = 0XFF & array[0];
            return (result > 0 ? value : result);
        } else {
            throw new IOException("stream closed");
        }
    }

    @Override
    public int read(final byte[] dest, final int offs, final int len) throws IOException {
        if (offs < 0) {
            throw new IndexOutOfBoundsException("offs(" + offs + ") < 0.");
        }
        if (len < 0) {
            throw new IndexOutOfBoundsException("len(" + len + ") < 0.");
        }
        if (offs + len > dest.length) {
            throw new IndexOutOfBoundsException("offs(" + offs + ") + len(" + len + ") > dest.length(" + dest.length + ").");
        }
        if (this.in == null) {
            throw new IOException("stream closed");
        }

        if (lazyInitialization) {
            this.init();
            this.lazyInitialization = false;
        }

        if (skipDecompression) {
            changeStateToProcessABlock();
            skipDecompression = false;
        }

        final int hi = offs + len;
        int destOffs = offs;
        int b = 0;

        for (; ((destOffs < hi) && ((b = read0())) >= 0);) {
            dest[destOffs++] = (byte) b;
        }

        int result = destOffs - offs;
        if (result == 0) {
            result = b;
            skipResult = skipToNextBlockMarker();
            changeStateToProcessABlock();
        }
        return result;
    }

    private int read0() throws IOException {
        final int retChar = this.currentChar;

        switch (this.currentState) {
            case EOF:
                return END_OF_STREAM;
            case NO_PROCESS_STATE:
                return END_OF_BLOCK;
            case START_BLOCK_STATE:
                throw new IllegalStateException();
            case RAND_PART_A_STATE:
                throw new IllegalStateException();
            case RAND_PART_B_STATE:
                setupRandPartB();
                break;
            case RAND_PART_C_STATE:
                setupRandPartC();
                break;
            case NO_RAND_PART_A_STATE:
                throw new IllegalStateException();
            case NO_RAND_PART_B_STATE:
                setupNoRandPartB();
                break;
            case NO_RAND_PART_C_STATE:
                setupNoRandPartC();
                break;
            default:
                throw new IllegalStateException();
        }

        return retChar;
    }

    private void init() throws IOException {
        int magic2 = this.readAByte(in);
        if (magic2 != 'h') {
            throw new IOException("Stream is not BZip2 formatted: expected 'h'" + " as first byte but got '" + (char) magic2 + "'");
        }

        int blockSize = this.readAByte(in);
        if ((blockSize < '1') || (blockSize > '9')) {
            throw new IOException("Stream is not BZip2 formatted: illegal " + "blocksize " + (char) blockSize);
        }

        this.blockSize100k = blockSize - '0';

        initBlock();
        setupBlock();
    }

    private void initBlock() throws IOException {
        if (this.readMode == ReadMode.BYBLOCK) {
            this.storedBlockCRC = bsGetInt();
            this.blockRandomised = bsR(1) == 1;

            if (this.data == null) {
                this.data = new Data(this.blockSize100k);
            }

            getAndMoveToFrontDecode();

            this.crc.initialiseCRC();
            this.currentState = STATE.START_BLOCK_STATE;
            return;
        }

        char magic0 = bsGetUByte();
        char magic1 = bsGetUByte();
        char magic2 = bsGetUByte();
        char magic3 = bsGetUByte();
        char magic4 = bsGetUByte();
        char magic5 = bsGetUByte();

        if (magic0 == 0x17 && magic1 == 0x72 && magic2 == 0x45 && magic3 == 0x38 && magic4 == 0x50 && magic5 == 0x90) {
            complete();
        } else if (magic0 != 0x31 || magic1 != 0x41 || magic2 != 0x59 || magic3 != 0x26 || magic4 != 0x53 || magic5 != 0x59) {
            this.currentState = STATE.EOF;
            throw new IOException("bad block header");
        } else {
            this.storedBlockCRC = bsGetInt();
            this.blockRandomised = bsR(1) == 1;

            if (this.data == null) {
                this.data = new Data(this.blockSize100k);
            }

            getAndMoveToFrontDecode();

            this.crc.initialiseCRC();
            this.currentState = STATE.START_BLOCK_STATE;
        }
    }

    private void endBlock() throws IOException {
        this.computedBlockCRC = this.crc.getFinalCRC();

        if (this.storedBlockCRC != this.computedBlockCRC) {
            this.computedCombinedCRC = (this.storedCombinedCRC << 1) | (this.storedCombinedCRC >>> 31);
            this.computedCombinedCRC ^= this.storedBlockCRC;

            reportCRCError();
        }

        this.computedCombinedCRC = (this.computedCombinedCRC << 1) | (this.computedCombinedCRC >>> 31);
        this.computedCombinedCRC ^= this.computedBlockCRC;
    }

    private void complete() throws IOException {
        this.storedCombinedCRC = bsGetInt();
        this.currentState = STATE.EOF;
        this.data = null;

        if (this.storedCombinedCRC != this.computedCombinedCRC) {
            reportCRCError();
        }
    }

    @Override
    public void close() throws IOException {
        InputStream inShadow = this.in;
        if (inShadow != null) {
            try {
                if (inShadow != System.in) {
                    inShadow.close();
                }
            } finally {
                this.data = null;
                this.in = null;
            }
        }
    }

    private long bsR(final long n) throws IOException {
        long bsLiveShadow = this.bsLive;
        long bsBuffShadow = this.bsBuff;

        if (bsLiveShadow < n) {
            final InputStream inShadow = this.in;
            do {
                int thech = readAByte(inShadow);

                if (thech < 0) {
                    throw new IOException("unexpected end of stream");
                }

                bsBuffShadow = (bsBuffShadow << 8) | thech;
                bsLiveShadow += 8;
            } while (bsLiveShadow < n);

            this.bsBuff = bsBuffShadow;
        }

        this.bsLive = bsLiveShadow - n;
        return (bsBuffShadow >> (bsLiveShadow - n)) & ((1L << n) - 1);
    }

    private boolean bsGetBit() throws IOException {
        long bsLiveShadow = this.bsLive;
        long bsBuffShadow = this.bsBuff;

        if (bsLiveShadow < 1) {
            int thech = this.readAByte(in);

            if (thech < 0) {
                throw new IOException("unexpected end of stream");
            }

            bsBuffShadow = (bsBuffShadow << 8) | thech;
            bsLiveShadow += 8;
            this.bsBuff = bsBuffShadow;
        }

        this.bsLive = bsLiveShadow - 1;
        return ((bsBuffShadow >> (bsLiveShadow - 1)) & 1) != 0;
    }

    private char bsGetUByte() throws IOException {
        return (char) bsR(8);
    }

    private int bsGetInt() throws IOException {
        return (int) ((((((bsR(8) << 8) | bsR(8)) << 8) | bsR(8)) << 8) | bsR(8));
    }

    private static void hbCreateDecodeTables(
        final int[] limit,
        final int[] base,
        final int[] perm,
        final char[] length,
        final int minLen,
        final int maxLen,
        final int alphaSize
    ) {
        for (int i = minLen, pp = 0; i <= maxLen; i++) {
            for (int j = 0; j < alphaSize; j++) {
                if (length[j] == i) {
                    perm[pp++] = j;
                }
            }
        }

        for (int i = MAX_CODE_LEN; --i > 0;) {
            base[i] = 0;
            limit[i] = 0;
        }

        for (int i = 0; i < alphaSize; i++) {
            base[length[i] + 1]++;
        }

        for (int i = 1, b = base[0]; i < MAX_CODE_LEN; i++) {
            b += base[i];
            base[i] = b;
        }

        for (int i = minLen, vec = 0, b = base[i]; i <= maxLen; i++) {
            final int nb = base[i + 1];
            vec += nb - b;
            b = nb;
            limit[i] = vec - 1;
            vec <<= 1;
        }

        for (int i = minLen + 1; i <= maxLen; i++) {
            base[i] = ((limit[i - 1] + 1) << 1) - base[i];
        }
    }

    private void recvDecodingTables() throws IOException {
        final Data dataShadow = this.data;
        final boolean[] inUse = dataShadow.inUse;
        final byte[] pos = dataShadow.recvDecodingTables_pos;
        final byte[] selector = dataShadow.selector;
        final byte[] selectorMtf = dataShadow.selectorMtf;

        int inUse16 = 0;

        for (int i = 0; i < 16; i++) {
            if (bsGetBit()) {
                inUse16 |= 1 << i;
            }
        }

        for (int i = 256; --i >= 0;) {
            inUse[i] = false;
        }

        for (int i = 0; i < 16; i++) {
            if ((inUse16 & (1 << i)) != 0) {
                final int i16 = i << 4;
                for (int j = 0; j < 16; j++) {
                    if (bsGetBit()) {
                        inUse[i16 + j] = true;
                    }
                }
            }
        }

        makeMaps();
        final int alphaSize = this.nInUse + 2;

        final int nGroups = (int) bsR(3);
        final int nSelectors = (int) bsR(15);

        for (int i = 0; i < nSelectors; i++) {
            int j = 0;
            while (bsGetBit()) {
                j++;
            }
            selectorMtf[i] = (byte) j;
        }

        for (int v = nGroups; --v >= 0;) {
            pos[v] = (byte) v;
        }

        for (int i = 0; i < nSelectors; i++) {
            int v = selectorMtf[i] & 0xff;
            final byte tmp = pos[v];
            while (v > 0) {
                pos[v] = pos[v - 1];
                v--;
            }
            pos[0] = tmp;
            selector[i] = tmp;
        }

        final char[][] len = dataShadow.temp_charArray2d;

        for (int t = 0; t < nGroups; t++) {
            int curr = (int) bsR(5);
            final char[] len_t = len[t];
            for (int i = 0; i < alphaSize; i++) {
                while (bsGetBit()) {
                    curr += bsGetBit() ? -1 : 1;
                }
                len_t[i] = (char) curr;
            }
        }

        createHuffmanDecodingTables(alphaSize, nGroups);
    }

    private void createHuffmanDecodingTables(final int alphaSize, final int nGroups) {
        final Data dataShadow = this.data;
        final char[][] len = dataShadow.temp_charArray2d;
        final int[] minLens = dataShadow.minLens;
        final int[][] limit = dataShadow.limit;
        final int[][] base = dataShadow.base;
        final int[][] perm = dataShadow.perm;

        for (int t = 0; t < nGroups; t++) {
            int minLen = 32;
            int maxLen = 0;
            final char[] len_t = len[t];
            for (int i = alphaSize; --i >= 0;) {
                final char lent = len_t[i];
                if (lent > maxLen) {
                    maxLen = lent;
                }
                if (lent < minLen) {
                    minLen = lent;
                }
            }
            hbCreateDecodeTables(limit[t], base[t], perm[t], len[t], minLen, maxLen, alphaSize);
            minLens[t] = minLen;
        }
    }

    private void getAndMoveToFrontDecode() throws IOException {
        this.origPtr = (int) bsR(24);
        recvDecodingTables();

        final InputStream inShadow = this.in;
        final Data dataShadow = this.data;
        final byte[] ll8 = dataShadow.ll8;
        final int[] unzftab = dataShadow.unzftab;
        final byte[] selector = dataShadow.selector;
        final byte[] seqToUnseq = dataShadow.seqToUnseq;
        final char[] yy = dataShadow.getAndMoveToFrontDecode_yy;
        final int[] minLens = dataShadow.minLens;
        final int[][] limit = dataShadow.limit;
        final int[][] base = dataShadow.base;
        final int[][] perm = dataShadow.perm;
        final int limitLast = this.blockSize100k * 100000;

        for (int i = 256; --i >= 0;) {
            yy[i] = (char) i;
            unzftab[i] = 0;
        }

        int groupNo = 0;
        int groupPos = G_SIZE - 1;
        final int eob = this.nInUse + 1;
        int nextSym = getAndMoveToFrontDecode0(0);
        int bsBuffShadow = (int) this.bsBuff;
        int bsLiveShadow = (int) this.bsLive;
        int lastShadow = -1;
        int zt = selector[groupNo] & 0xff;
        int[] base_zt = base[zt];
        int[] limit_zt = limit[zt];
        int[] perm_zt = perm[zt];
        int minLens_zt = minLens[zt];

        while (nextSym != eob) {
            if ((nextSym == RUNA) || (nextSym == RUNB)) {
                int s = -1;

                for (int n = 1; true; n <<= 1) {
                    if (nextSym == RUNA) {
                        s += n;
                    } else if (nextSym == RUNB) {
                        s += n << 1;
                    } else {
                        break;
                    }

                    if (groupPos == 0) {
                        groupPos = G_SIZE - 1;
                        zt = selector[++groupNo] & 0xff;
                        base_zt = base[zt];
                        limit_zt = limit[zt];
                        perm_zt = perm[zt];
                        minLens_zt = minLens[zt];
                    } else {
                        groupPos--;
                    }

                    int zn = minLens_zt;

                    while (bsLiveShadow < zn) {
                        final int thech = readAByte(inShadow);
                        if (thech >= 0) {
                            bsBuffShadow = (bsBuffShadow << 8) | thech;
                            bsLiveShadow += 8;
                            continue;
                        } else {
                            throw new IOException("unexpected end of stream");
                        }
                    }
                    long zvec = (bsBuffShadow >> (bsLiveShadow - zn)) & ((1 << zn) - 1);
                    bsLiveShadow -= zn;

                    while (zvec > limit_zt[zn]) {
                        zn++;
                        while (bsLiveShadow < 1) {
                            final int thech = readAByte(inShadow);
                            if (thech >= 0) {
                                bsBuffShadow = (bsBuffShadow << 8) | thech;
                                bsLiveShadow += 8;
                                continue;
                            } else {
                                throw new IOException("unexpected end of stream");
                            }
                        }
                        bsLiveShadow--;
                        zvec = (zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1);
                    }
                    nextSym = perm_zt[(int) (zvec - base_zt[zn])];
                }

                final byte ch = seqToUnseq[yy[0]];
                unzftab[ch & 0xff] += s + 1;

                while (s-- >= 0) {
                    ll8[++lastShadow] = ch;
                }

                if (lastShadow >= limitLast) {
                    throw new IOException("block overrun");
                }
            } else {
                if (++lastShadow >= limitLast) {
                    throw new IOException("block overrun");
                }

                final char tmp = yy[nextSym - 1];
                unzftab[seqToUnseq[tmp] & 0xff]++;
                ll8[lastShadow] = seqToUnseq[tmp];

                if (nextSym <= 16) {
                    for (int j = nextSym - 1; j > 0;) {
                        yy[j] = yy[--j];
                    }
                } else {
                    System.arraycopy(yy, 0, yy, 1, nextSym - 1);
                }

                yy[0] = tmp;

                if (groupPos == 0) {
                    groupPos = G_SIZE - 1;
                    zt = selector[++groupNo] & 0xff;
                    base_zt = base[zt];
                    limit_zt = limit[zt];
                    perm_zt = perm[zt];
                    minLens_zt = minLens[zt];
                } else {
                    groupPos--;
                }

                int zn = minLens_zt;

                while (bsLiveShadow < zn) {
                    final int thech = readAByte(inShadow);
                    if (thech >= 0) {
                        bsBuffShadow = (bsBuffShadow << 8) | thech;
                        bsLiveShadow += 8;
                        continue;
                    } else {
                        throw new IOException("unexpected end of stream");
                    }
                }
                int zvec = (bsBuffShadow >> (bsLiveShadow - zn)) & ((1 << zn) - 1);
                bsLiveShadow -= zn;

                while (zvec > limit_zt[zn]) {
                    zn++;
                    while (bsLiveShadow < 1) {
                        final int thech = readAByte(inShadow);
                        if (thech >= 0) {
                            bsBuffShadow = (bsBuffShadow << 8) | thech;
                            bsLiveShadow += 8;
                            continue;
                        } else {
                            throw new IOException("unexpected end of stream");
                        }
                    }
                    bsLiveShadow--;
                    zvec = ((zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1));
                }
                nextSym = perm_zt[zvec - base_zt[zn]];
            }
        }

        this.last = lastShadow;
        this.bsLive = bsLiveShadow;
        this.bsBuff = bsBuffShadow;
    }

    private int getAndMoveToFrontDecode0(final int groupNo) throws IOException {
        final InputStream inShadow = this.in;
        final Data dataShadow = this.data;
        final int zt = dataShadow.selector[groupNo] & 0xff;
        final int[] limit_zt = dataShadow.limit[zt];
        int zn = dataShadow.minLens[zt];
        int zvec = (int) bsR(zn);
        int bsLiveShadow = (int) this.bsLive;
        int bsBuffShadow = (int) this.bsBuff;

        while (zvec > limit_zt[zn]) {
            zn++;
            while (bsLiveShadow < 1) {
                final int thech = readAByte(inShadow);

                if (thech >= 0) {
                    bsBuffShadow = (bsBuffShadow << 8) | thech;
                    bsLiveShadow += 8;
                    continue;
                } else {
                    throw new IOException("unexpected end of stream");
                }
            }
            bsLiveShadow--;
            zvec = (zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1);
        }

        this.bsLive = bsLiveShadow;
        this.bsBuff = bsBuffShadow;

        return dataShadow.perm[zt][zvec - dataShadow.base[zt][zn]];
    }

    private void setupBlock() throws IOException {
        if (this.data == null) {
            return;
        }

        final int[] cftab = this.data.cftab;
        final int[] tt = this.data.initTT(this.last + 1);
        final byte[] ll8 = this.data.ll8;
        cftab[0] = 0;
        System.arraycopy(this.data.unzftab, 0, cftab, 1, 256);

        for (int i = 1, c = cftab[0]; i <= 256; i++) {
            c += cftab[i];
            cftab[i] = c;
        }

        for (int i = 0, lastShadow = this.last; i <= lastShadow; i++) {
            tt[cftab[ll8[i] & 0xff]++] = i;
        }

        if ((this.origPtr < 0) || (this.origPtr >= tt.length)) {
            throw new IOException("stream corrupted");
        }

        this.su_tPos = tt[this.origPtr];
        this.su_count = 0;
        this.su_i2 = 0;
        this.su_ch2 = 256;

        if (this.blockRandomised) {
            this.su_rNToGo = 0;
            this.su_rTPos = 0;
            setupRandPartA();
        } else {
            setupNoRandPartA();
        }
    }

    private void setupRandPartA() throws IOException {
        if (this.su_i2 <= this.last) {
            this.su_chPrev = this.su_ch2;
            int su_ch2Shadow = this.data.ll8[this.su_tPos] & 0xff;
            this.su_tPos = this.data.tt[this.su_tPos];
            if (this.su_rNToGo == 0) {
                this.su_rNToGo = BZip2Constants.rNums[this.su_rTPos] - 1;
                if (++this.su_rTPos == 512) {
                    this.su_rTPos = 0;
                }
            } else {
                this.su_rNToGo--;
            }
            this.su_ch2 = su_ch2Shadow ^= (this.su_rNToGo == 1) ? 1 : 0;
            this.su_i2++;
            this.currentChar = su_ch2Shadow;
            this.currentState = STATE.RAND_PART_B_STATE;
            this.crc.updateCRC(su_ch2Shadow);
        } else {
            endBlock();
            if (readMode == ReadMode.CONTINUOUS) {
                initBlock();
                setupBlock();
            } else if (readMode == ReadMode.BYBLOCK) {
                this.currentState = STATE.NO_PROCESS_STATE;
            }
        }
    }

    private void setupNoRandPartA() throws IOException {
        if (this.su_i2 <= this.last) {
            this.su_chPrev = this.su_ch2;
            int su_ch2Shadow = this.data.ll8[this.su_tPos] & 0xff;
            this.su_ch2 = su_ch2Shadow;
            this.su_tPos = this.data.tt[this.su_tPos];
            this.su_i2++;
            this.currentChar = su_ch2Shadow;
            this.currentState = STATE.NO_RAND_PART_B_STATE;
            this.crc.updateCRC(su_ch2Shadow);
        } else {
            this.currentState = STATE.NO_RAND_PART_A_STATE;
            endBlock();
            if (readMode == ReadMode.CONTINUOUS) {
                initBlock();
                setupBlock();
            } else if (readMode == ReadMode.BYBLOCK) {
                this.currentState = STATE.NO_PROCESS_STATE;
            }
        }
    }

    private void setupRandPartB() throws IOException {
        if (this.su_ch2 != this.su_chPrev) {
            this.currentState = STATE.RAND_PART_A_STATE;
            this.su_count = 1;
            setupRandPartA();
        } else if (++this.su_count >= 4) {
            this.su_z = (char) (this.data.ll8[this.su_tPos] & 0xff);
            this.su_tPos = this.data.tt[this.su_tPos];
            if (this.su_rNToGo == 0) {
                this.su_rNToGo = BZip2Constants.rNums[this.su_rTPos] - 1;
                if (++this.su_rTPos == 512) {
                    this.su_rTPos = 0;
                }
            } else {
                this.su_rNToGo--;
            }
            this.su_j2 = 0;
            this.currentState = STATE.RAND_PART_C_STATE;
            if (this.su_rNToGo == 1) {
                this.su_z ^= 1;
            }
            setupRandPartC();
        } else {
            this.currentState = STATE.RAND_PART_A_STATE;
            setupRandPartA();
        }
    }

    private void setupRandPartC() throws IOException {
        if (this.su_j2 < this.su_z) {
            this.currentChar = this.su_ch2;
            this.crc.updateCRC(this.su_ch2);
            this.su_j2++;
        } else {
            this.currentState = STATE.RAND_PART_A_STATE;
            this.su_i2++;
            this.su_count = 0;
            setupRandPartA();
        }
    }

    private void setupNoRandPartB() throws IOException {
        if (this.su_ch2 != this.su_chPrev) {
            this.su_count = 1;
            setupNoRandPartA();
        } else if (++this.su_count >= 4) {
            this.su_z = (char) (this.data.ll8[this.su_tPos] & 0xff);
            this.su_tPos = this.data.tt[this.su_tPos];
            this.su_j2 = 0;
            setupNoRandPartC();
        } else {
            setupNoRandPartA();
        }
    }

    private void setupNoRandPartC() throws IOException {
        if (this.su_j2 < this.su_z) {
            int su_ch2Shadow = this.su_ch2;
            this.currentChar = su_ch2Shadow;
            this.crc.updateCRC(su_ch2Shadow);
            this.su_j2++;
            this.currentState = STATE.NO_RAND_PART_C_STATE;
        } else {
            this.su_i2++;
            this.su_count = 0;
            setupNoRandPartA();
        }
    }

    private static final class Data {

        final boolean[] inUse = new boolean[256];
        final byte[] seqToUnseq = new byte[256];
        final byte[] selector = new byte[MAX_SELECTORS];
        final byte[] selectorMtf = new byte[MAX_SELECTORS];
        final int[] unzftab = new int[256];
        final int[][] limit = new int[N_GROUPS][MAX_ALPHA_SIZE];
        final int[][] base = new int[N_GROUPS][MAX_ALPHA_SIZE];
        final int[][] perm = new int[N_GROUPS][MAX_ALPHA_SIZE];
        final int[] minLens = new int[N_GROUPS];
        final int[] cftab = new int[257];
        final char[] getAndMoveToFrontDecode_yy = new char[256];
        final char[][] temp_charArray2d = new char[N_GROUPS][MAX_ALPHA_SIZE];
        final byte[] recvDecodingTables_pos = new byte[N_GROUPS];

        int[] tt;
        byte[] ll8;

        Data(int blockSize100k) {
            super();
            this.ll8 = new byte[blockSize100k * BZip2Constants.baseBlockSize];
        }

        int[] initTT(int length) {
            int[] ttShadow = this.tt;

            if ((ttShadow == null) || (ttShadow.length < length)) {
                this.tt = ttShadow = new int[length];
            }

            return ttShadow;
        }
    }
}
