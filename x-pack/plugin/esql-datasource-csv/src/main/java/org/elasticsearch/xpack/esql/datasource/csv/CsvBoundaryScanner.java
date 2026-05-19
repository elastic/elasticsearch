/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

/**
 * Quote-aware record-boundary scanning over a contiguous {@code byte[]}.
 * <p>
 * Two implementations live side by side:
 * <ul>
 *   <li>{@link #PREFIX_XOR} — bit-parallel prefix-XOR over 64-byte blocks. The production default.</li>
 *   <li>{@link #REFERENCE} — per-byte scalar scanner, behaviourally identical to the original
 *       {@code findLastRecordBoundaryQuotedFieldsOnly} in {@code CsvFormatReader}. Used as the
 *       equivalence oracle in {@code CsvBoundaryScannerTests}; also reachable as an emergency
 *       kill-switch via {@value #FAST_PATH_PROPERTY} at JVM start.</li>
 * </ul>
 * <p>
 * The choice between the two is made <b>once</b>, by {@code CsvFormatReader} at class-init time —
 * the active implementation is held in a {@code static final} field on {@code CsvFormatReader}, so
 * the JIT sees a constant {@link Impl} reference at every dispatch site and inlines through it.
 * <p>
 * Both implementations honour:
 * <ul>
 *   <li><b>Open-tail rule</b>: when the buffer ends mid-quote, the last record terminator before
 *       the open region is returned (or {@code -1}). Never an offset inside the open region.</li>
 *   <li><b>RFC 4180 doubled-quote escape</b>: {@code ""} inside a quoted field is a literal quote;
 *       the run stays open across the pair.</li>
 *   <li><b>Configurable quote character</b>: any single byte; threaded through both paths.</li>
 * </ul>
 * <p>
 * Out of scope: the bracket-multi-value-cell state machine ({@code [..]}-bounded regions in
 * {@code findNextRecordBoundaryBracketCommaMvc}). That path stays on the SPI default polyloop in
 * {@code CsvFormatReader}.
 */
public final class CsvBoundaryScanner {

    /** Number of bytes processed per prefix-XOR block in the fast path. */
    static final int BLOCK = 64;

    /**
     * System property controlling which implementation {@code CsvFormatReader} resolves at class
     * init. {@code true} (the default) selects {@link #PREFIX_XOR}; {@code false} selects
     * {@link #REFERENCE}. Read once at JVM start by {@code CsvFormatReader}; flipping it at
     * runtime has no effect on already-loaded readers.
     */
    public static final String FAST_PATH_PROPERTY = "esql.csv.fast_boundary_scanner";

    /** Bit-parallel prefix-XOR implementation. Stateless, thread-safe. */
    public static final Impl PREFIX_XOR = new PrefixXorImpl();

    /** Per-byte scalar reference, byte-for-byte equivalent to the original scanner. Stateless, thread-safe. */
    public static final Impl REFERENCE = new ReferenceImpl();

    private CsvBoundaryScanner() {}

    /**
     * Quote-aware record-boundary scanner. Both implementations must agree byte-for-byte on every
     * input the original {@code findLastRecordBoundaryQuotedFieldsOnly} handled correctly — that
     * contract is the property test in {@code CsvBoundaryScannerTests}.
     */
    public interface Impl {

        /**
         * Offset (relative to {@code off}) of the last {@code \n} byte that is outside any quoted
         * field within {@code buf[off..off+len)}, or {@code -1} if no such byte exists. Honours the
         * open-tail rule (see class-level javadoc).
         */
        int findLastRealTerminator(byte[] buf, int off, int len, byte quoteChar);

        /**
         * Count of {@code \n} bytes outside any quoted field within {@code buf[off..off+len)}.
         * Threads {@code carryIn} as the entering in-quote state ({@code 0L} = outside,
         * {@code -1L} = inside) and writes the exit state into {@code carryOut[0]} when
         * {@code carryOut != null}. Designed for streaming use across multiple buffer reads.
         */
        long countRealTerminators(byte[] buf, int off, int len, byte quoteChar, long carryIn, long[] carryOut);
    }

    // -------- Fast path: prefix-XOR over 64-byte blocks ----------------------------------------

    private static final class PrefixXorImpl implements Impl {

        @Override
        public int findLastRealTerminator(byte[] buf, int off, int len, byte quoteChar) {
            if (len <= 0) {
                return -1;
            }
            long carry = 0L;
            int lastBoundary = -1;
            int i = 0;
            while (i + BLOCK <= len) {
                long quoteBits = byteMask(buf, off + i, quoteChar);
                long newlineBits = byteMask(buf, off + i, (byte) '\n');

                long inQuote = prefixXor(quoteBits) ^ carry;
                long realTerminators = newlineBits & ~inQuote;
                if (realTerminators != 0L) {
                    int highBit = 63 - Long.numberOfLeadingZeros(realTerminators);
                    lastBoundary = i + highBit;
                }
                carry = -((inQuote >>> 63) & 1L);
                i += BLOCK;
            }

            // Tail: 0..63 leftover bytes. Per-byte scan, seeded by the in-quote carry from the
            // last full block.
            boolean inQuoteScalar = (carry != 0L);
            while (i < len) {
                byte b = buf[off + i];
                if (b == quoteChar) {
                    if (inQuoteScalar) {
                        if (i + 1 < len && buf[off + i + 1] == quoteChar) {
                            i++; // RFC 4180 doubled-quote: literal, stay in run.
                        } else {
                            inQuoteScalar = false;
                        }
                    } else {
                        inQuoteScalar = true;
                    }
                } else if (b == '\n' && inQuoteScalar == false) {
                    lastBoundary = i;
                }
                i++;
            }
            return lastBoundary;
        }

        @Override
        public long countRealTerminators(byte[] buf, int off, int len, byte quoteChar, long carryIn, long[] carryOut) {
            if (len <= 0) {
                if (carryOut != null) {
                    carryOut[0] = carryIn;
                }
                return 0L;
            }
            long carry = carryIn;
            long count = 0L;
            int i = 0;
            while (i + BLOCK <= len) {
                long quoteBits = byteMask(buf, off + i, quoteChar);
                long newlineBits = byteMask(buf, off + i, (byte) '\n');

                long inQuote = prefixXor(quoteBits) ^ carry;
                count += Long.bitCount(newlineBits & ~inQuote);
                carry = -((inQuote >>> 63) & 1L);
                i += BLOCK;
            }
            boolean inQuoteScalar = (carry != 0L);
            while (i < len) {
                byte b = buf[off + i];
                if (b == quoteChar) {
                    if (inQuoteScalar) {
                        if (i + 1 < len && buf[off + i + 1] == quoteChar) {
                            i++;
                        } else {
                            inQuoteScalar = false;
                        }
                    } else {
                        inQuoteScalar = true;
                    }
                } else if (b == '\n' && inQuoteScalar == false) {
                    count++;
                }
                i++;
            }
            if (carryOut != null) {
                carryOut[0] = inQuoteScalar ? -1L : 0L;
            }
            return count;
        }

        /**
         * Build a bitmap of {@link #BLOCK} bits: bit {@code j} set iff {@code buf[off + j] == target}.
         * Pure scalar — the Vector API variant in a follow-up commit replaces this method with a
         * parallel byte-compare.
         */
        private static long byteMask(byte[] buf, int off, byte target) {
            long mask = 0L;
            for (int j = 0; j < BLOCK; j++) {
                if (buf[off + j] == target) {
                    mask |= 1L << j;
                }
            }
            return mask;
        }

        /** Hillis-Steele prefix-XOR scan on a 64-bit word. */
        private static long prefixXor(long x) {
            x ^= x << 1;
            x ^= x << 2;
            x ^= x << 4;
            x ^= x << 8;
            x ^= x << 16;
            x ^= x << 32;
            return x;
        }
    }

    // -------- Reference path: per-byte scalar (the original scanner, preserved) ----------------

    private static final class ReferenceImpl implements Impl {

        @Override
        public int findLastRealTerminator(byte[] buf, int off, int len, byte quoteChar) {
            if (len <= 0) {
                return -1;
            }
            int lastBoundary = -1;
            boolean inQuotes = false;
            for (int i = 0; i < len; i++) {
                byte b = buf[off + i];
                if (b == quoteChar) {
                    if (inQuotes) {
                        if (i + 1 < len && buf[off + i + 1] == quoteChar) {
                            // Doubled quote inside a quoted field — RFC 4180 literal, stay in quotes.
                            i++;
                        } else {
                            inQuotes = false;
                        }
                    } else {
                        inQuotes = true;
                    }
                } else if (b == '\n' && inQuotes == false) {
                    lastBoundary = i;
                }
            }
            return lastBoundary;
        }

        @Override
        public long countRealTerminators(byte[] buf, int off, int len, byte quoteChar, long carryIn, long[] carryOut) {
            long count = 0L;
            boolean inQuotes = (carryIn != 0L);
            for (int i = 0; i < len; i++) {
                byte b = buf[off + i];
                if (b == quoteChar) {
                    if (inQuotes) {
                        if (i + 1 < len && buf[off + i + 1] == quoteChar) {
                            i++;
                        } else {
                            inQuotes = false;
                        }
                    } else {
                        inQuotes = true;
                    }
                } else if (b == '\n' && inQuotes == false) {
                    count++;
                }
            }
            if (carryOut != null) {
                carryOut[0] = inQuotes ? -1L : 0L;
            }
            return count;
        }
    }
}
