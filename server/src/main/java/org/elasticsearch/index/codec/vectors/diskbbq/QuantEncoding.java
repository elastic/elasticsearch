/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.simdvec.ESVectorUtil;

public enum QuantEncoding {
    ONE_BIT_4BIT_QUERY(0, (byte) 1, (byte) 4) {
        @Override
        public void pack(int[] quantized, byte[] destination) {
            ESVectorUtil.packAsBinary(quantized, destination);
        }

        @Override
        public void packQuery(int[] quantized, byte[] destination) {
            ESVectorUtil.transposeHalfByte(quantized, destination);
        }
    },
    TWO_BIT_4BIT_QUERY(1, (byte) 2, (byte) 4) {
        @Override
        public void pack(int[] quantized, byte[] destination) {
            ESVectorUtil.packDibitQuad(quantized, destination);
        }

        @Override
        public void packQuery(int[] quantized, byte[] destination) {
            packDibitQueryByStripe(quantized, destination);
        }

        @Override
        public int getDocPackedLength(int dimensions) {
            int discretized = discretizedDimensions(dimensions);
            return discretized / 4;
        }

        @Override
        public int getQueryPackedLength(int dimensions) {
            return discretizedDimensions(dimensions);
        }
    },
    FOUR_BIT_SYMMETRIC(2, (byte) 4, (byte) 4) {
        @Override
        public void packQuery(int[] quantized, byte[] destination) {
            ESVectorUtil.packAsBytes(quantized, destination, quantized.length);
        }

        @Override
        public void pack(int[] quantized, byte[] destination) {
            packNibbles(quantized, destination);
        }

        @Override
        public int getDocPackedLength(int dimensions) {
            int discretized = discretizedDimensions(dimensions);
            return discretized / 2;
        }

        @Override
        public int getQueryPackedLength(int dimensions) {
            return discretizedDimensions(dimensions);
        }

        @Override
        public int discretizedDimensions(int dimensions) {
            int totalBits = dimensions * 4;
            return (totalBits + 7) / 8 * 8 / 4;
        }
    },
    SEVEN_BIT_SYMMETRIC(3, (byte) 7, (byte) 7) {
        @Override
        public void pack(int[] quantized, byte[] destination) {
            ESVectorUtil.packAsBytes(quantized, destination, quantized.length);
        }

        @Override
        public void packQuery(int[] quantized, byte[] destination) {
            ESVectorUtil.packAsBytes(quantized, destination, quantized.length);
        }

        @Override
        public int discretizedDimensions(int dimensions) {
            return dimensions;
        }

        @Override
        public int getDocPackedLength(int dimensions) {
            return discretizedDimensions(dimensions);
        }

        @Override
        public int getQueryPackedLength(int dimensions) {
            return discretizedDimensions(dimensions);
        }
    },
    ONE_BIT_1BIT_QUERY(5, (byte) 1, (byte) 1) {
        @Override
        public void pack(int[] quantized, byte[] destination) {
            ESVectorUtil.packAsBinary(quantized, destination);
        }

        @Override
        public void packQuery(int[] quantized, byte[] destination) {
            ESVectorUtil.packAsBinary(quantized, destination);
        }
    };

    private static void packNibbles(int[] quantized, byte[] destination) {
        assert quantized.length == destination.length * 2;
        int packedLength = destination.length;
        for (int i = 0; i < packedLength; i++) {
            destination[i] = (byte) ((quantized[i] << 4) | (quantized[packedLength + i] & 0x0F));
        }
    }

    private static void packDibitQueryByStripe(int[] quantized, byte[] destination) {
        assert quantized.length == destination.length;
        assert destination.length % 4 == 0;
        int packedLength = destination.length / 4;
        for (int i = 0; i < packedLength; i++) {
            destination[i] = (byte) quantized[4 * i];
            destination[i + packedLength] = (byte) quantized[4 * i + 1];
            destination[i + 2 * packedLength] = (byte) quantized[4 * i + 2];
            destination[i + 3 * packedLength] = (byte) quantized[4 * i + 3];
        }
    }

    private final int id;
    private final byte bits, queryBits;

    QuantEncoding(int id, byte bits, byte queryBits) {
        this.id = id;
        this.bits = bits;
        this.queryBits = queryBits;
    }

    public abstract void pack(int[] quantized, byte[] destination);

    public abstract void packQuery(int[] quantized, byte[] destination);

    public int id() {
        return id;
    }

    public byte bits() {
        return bits;
    }

    public byte queryBits() {
        return queryBits;
    }

    public int discretizedDimensions(int dimensions) {
        if (queryBits == bits) {
            int totalBits = dimensions * bits;
            return (totalBits + 7) / 8 * 8 / bits;
        }
        int queryDiscretized = (dimensions * queryBits + 7) / 8 * 8 / queryBits;
        int docDiscretized = (dimensions * bits + 7) / 8 * 8 / bits;
        int maxDiscretized = Math.max(queryDiscretized, docDiscretized);
        assert maxDiscretized % (8.0 / queryBits) == 0 : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
        assert maxDiscretized % (8.0 / bits) == 0 : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
        return maxDiscretized;
    }

    /** Return the number of bytes required to store a packed vector of the given dimensions. */
    public int getDocPackedLength(int dimensions) {
        int discretized = discretizedDimensions(dimensions);
        // how many bytes do we need to store the quantized vector?
        int totalBits = discretized * bits;
        return (totalBits + 7) / 8;
    }

    public int getQueryPackedLength(int dimensions) {
        int discretized = discretizedDimensions(dimensions);
        // how many bytes do we need to store the quantized vector?
        int totalBits = discretized * queryBits;
        return (totalBits + 7) / 8;
    }

    public static QuantEncoding fromId(int id) {
        for (QuantEncoding encoding : values()) {
            if (encoding.id == id) {
                return encoding;
            }
        }
        throw new IllegalArgumentException("Unknown QuantEncoding id: " + id);
    }

    public static QuantEncoding fromBits(byte bits) {
        return switch (bits) {
            case 1 -> ONE_BIT_4BIT_QUERY;
            case 2 -> TWO_BIT_4BIT_QUERY;
            case 4 -> FOUR_BIT_SYMMETRIC;
            case 7 -> SEVEN_BIT_SYMMETRIC;
            default -> throw new IllegalArgumentException("Unsupported bits: " + bits);
        };
    }

    /**
     * Resolves the quantization encoding from document and query bit widths.
     *
     * @throws IllegalArgumentException if the combination is unsupported
     */
    public static QuantEncoding fromDocAndQueryBits(byte docBits, byte queryBits) {
        return switch (docBits) {
            case 1 -> {
                if (queryBits == 1) {
                    yield ONE_BIT_1BIT_QUERY;
                }
                if (queryBits == 4) {
                    yield ONE_BIT_4BIT_QUERY;
                }
                throw new IllegalArgumentException("1-bit document quantization supports query bits 1 or 4, but got: " + queryBits);
            }
            case 2 -> {
                if (queryBits != 4) {
                    throw new IllegalArgumentException(
                        "2-bit document quantization requires 4-bit query quantization, but got: " + queryBits
                    );
                }
                yield TWO_BIT_4BIT_QUERY;
            }
            case 4 -> {
                if (queryBits != 4) {
                    throw new IllegalArgumentException("4-bit symmetric quantization requires query bits 4, but got: " + queryBits);
                }
                yield FOUR_BIT_SYMMETRIC;
            }
            case 7 -> {
                if (queryBits != 7) {
                    throw new IllegalArgumentException("7-bit symmetric quantization requires query bits 7, but got: " + queryBits);
                }
                yield SEVEN_BIT_SYMMETRIC;
            }
            default -> throw new IllegalArgumentException("Unsupported document bits: " + docBits);
        };
    }
}
