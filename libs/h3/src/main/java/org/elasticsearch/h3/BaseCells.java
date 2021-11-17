/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.h3;

/**
 *  Base cell related lookup tables and access functions.
 */
class BaseCells {

    private static class BaseCellData {
        // "home" face and normalized ijk coordinates on that face
        final int homeFace;
        final int homeI;
        final int homeJ;
        final int homeK;
        // is this base cell a pentagon?
        final boolean isPentagon;
        // if a pentagon, what are its two clockwise offset
        final int[] cwOffsetPent;

        /// faces?
        BaseCellData(int homeFace, int homeI, int homeJ, int homeK, boolean isPentagon, int[] cwOffsetPent) {
            this.homeFace = homeFace;
            this.homeI = homeI;
            this.homeJ = homeJ;
            this.homeK = homeK;
            this.isPentagon = isPentagon;
            this.cwOffsetPent = cwOffsetPent;
        }
    }

    /**
     * Resolution 0 base cell data table.
     * <p>
     * For each base cell, gives the "home" face and ijk+ coordinates on that face,
     * whether or not the base cell is a pentagon. Additionally, if the base cell
     * is a pentagon, the two cw offset rotation adjacent faces are given (-1
     * indicates that no cw offset rotation faces exist for this base cell).
     */
    private static final BaseCellData[] baseCellData = new BaseCellData[] {
        new BaseCellData(1, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 0
        new BaseCellData(2, 1, 1, 0, false, new int[] { 0, 0 }),     // base cell 1
        new BaseCellData(1, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 2
        new BaseCellData(2, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 3
        new BaseCellData(0, 2, 0, 0, true, new int[] { -1, -1 }),   // base cell 4
        new BaseCellData(1, 1, 1, 0, false, new int[] { 0, 0 }),     // base cell 5
        new BaseCellData(1, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 6
        new BaseCellData(2, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 7
        new BaseCellData(0, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 8
        new BaseCellData(2, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 9
        new BaseCellData(1, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 10
        new BaseCellData(1, 0, 1, 1, false, new int[] { 0, 0 }),     // base cell 11
        new BaseCellData(3, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 12
        new BaseCellData(3, 1, 1, 0, false, new int[] { 0, 0 }),     // base cell 13
        new BaseCellData(11, 2, 0, 0, true, new int[] { 2, 6 }),    // base cell 14
        new BaseCellData(4, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 15
        new BaseCellData(0, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 16
        new BaseCellData(6, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 17
        new BaseCellData(0, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 18
        new BaseCellData(2, 0, 1, 1, false, new int[] { 0, 0 }),     // base cell 19
        new BaseCellData(7, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 20
        new BaseCellData(2, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 21
        new BaseCellData(0, 1, 1, 0, false, new int[] { 0, 0 }),     // base cell 22
        new BaseCellData(6, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 23
        new BaseCellData(10, 2, 0, 0, true, new int[] { 1, 5 }),    // base cell 24
        new BaseCellData(6, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 25
        new BaseCellData(3, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 26
        new BaseCellData(11, 1, 0, 0, false, new int[] { 0, 0 }),    // base cell 27
        new BaseCellData(4, 1, 1, 0, false, new int[] { 0, 0 }),     // base cell 28
        new BaseCellData(3, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 29
        new BaseCellData(0, 0, 1, 1, false, new int[] { 0, 0 }),     // base cell 30
        new BaseCellData(4, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 31
        new BaseCellData(5, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 32
        new BaseCellData(0, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 33
        new BaseCellData(7, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 34
        new BaseCellData(11, 1, 1, 0, false, new int[] { 0, 0 }),    // base cell 35
        new BaseCellData(7, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 36
        new BaseCellData(10, 1, 0, 0, false, new int[] { 0, 0 }),    // base cell 37
        new BaseCellData(12, 2, 0, 0, true, new int[] { 3, 7 }),    // base cell 38
        new BaseCellData(6, 1, 0, 1, false, new int[] { 0, 0 }),     // base cell 39
        new BaseCellData(7, 1, 0, 1, false, new int[] { 0, 0 }),     // base cell 40
        new BaseCellData(4, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 41
        new BaseCellData(3, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 42
        new BaseCellData(3, 0, 1, 1, false, new int[] { 0, 0 }),     // base cell 43
        new BaseCellData(4, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 44
        new BaseCellData(6, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 45
        new BaseCellData(11, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 46
        new BaseCellData(8, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 47
        new BaseCellData(5, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 48
        new BaseCellData(14, 2, 0, 0, true, new int[] { 0, 9 }),    // base cell 49
        new BaseCellData(5, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 50
        new BaseCellData(12, 1, 0, 0, false, new int[] { 0, 0 }),    // base cell 51
        new BaseCellData(10, 1, 1, 0, false, new int[] { 0, 0 }),    // base cell 52
        new BaseCellData(4, 0, 1, 1, false, new int[] { 0, 0 }),     // base cell 53
        new BaseCellData(12, 1, 1, 0, false, new int[] { 0, 0 }),    // base cell 54
        new BaseCellData(7, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 55
        new BaseCellData(11, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 56
        new BaseCellData(10, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 57
        new BaseCellData(13, 2, 0, 0, true, new int[] { 4, 8 }),    // base cell 58
        new BaseCellData(10, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 59
        new BaseCellData(11, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 60
        new BaseCellData(9, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 61
        new BaseCellData(8, 0, 1, 0, false, new int[] { 0, 0 }),     // base cell 62
        new BaseCellData(6, 2, 0, 0, true, new int[] { 11, 15 }),   // base cell 63
        new BaseCellData(8, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 64
        new BaseCellData(9, 0, 0, 1, false, new int[] { 0, 0 }),     // base cell 65
        new BaseCellData(14, 1, 0, 0, false, new int[] { 0, 0 }),    // base cell 66
        new BaseCellData(5, 1, 0, 1, false, new int[] { 0, 0 }),     // base cell 67
        new BaseCellData(16, 0, 1, 1, false, new int[] { 0, 0 }),    // base cell 68
        new BaseCellData(8, 1, 0, 1, false, new int[] { 0, 0 }),     // base cell 69
        new BaseCellData(5, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 70
        new BaseCellData(12, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 71
        new BaseCellData(7, 2, 0, 0, true, new int[] { 12, 16 }),   // base cell 72
        new BaseCellData(12, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 73
        new BaseCellData(10, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 74
        new BaseCellData(9, 0, 0, 0, false, new int[] { 0, 0 }),     // base cell 75
        new BaseCellData(13, 1, 0, 0, false, new int[] { 0, 0 }),    // base cell 76
        new BaseCellData(16, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 77
        new BaseCellData(15, 0, 1, 1, false, new int[] { 0, 0 }),    // base cell 78
        new BaseCellData(15, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 79
        new BaseCellData(16, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 80
        new BaseCellData(14, 1, 1, 0, false, new int[] { 0, 0 }),    // base cell 81
        new BaseCellData(13, 1, 1, 0, false, new int[] { 0, 0 }),    // base cell 82
        new BaseCellData(5, 2, 0, 0, true, new int[] { 10, 19 }),   // base cell 83
        new BaseCellData(8, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 84
        new BaseCellData(14, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 85
        new BaseCellData(9, 1, 0, 1, false, new int[] { 0, 0 }),     // base cell 86
        new BaseCellData(14, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 87
        new BaseCellData(17, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 88
        new BaseCellData(12, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 89
        new BaseCellData(16, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 90
        new BaseCellData(17, 0, 1, 1, false, new int[] { 0, 0 }),    // base cell 91
        new BaseCellData(15, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 92
        new BaseCellData(16, 1, 0, 1, false, new int[] { 0, 0 }),    // base cell 93
        new BaseCellData(9, 1, 0, 0, false, new int[] { 0, 0 }),     // base cell 94
        new BaseCellData(15, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 95
        new BaseCellData(13, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 96
        new BaseCellData(8, 2, 0, 0, true, new int[] { 13, 17 }),   // base cell 97
        new BaseCellData(13, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 98
        new BaseCellData(17, 1, 0, 1, false, new int[] { 0, 0 }),    // base cell 99
        new BaseCellData(19, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 100
        new BaseCellData(14, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 101
        new BaseCellData(19, 0, 1, 1, false, new int[] { 0, 0 }),    // base cell 102
        new BaseCellData(17, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 103
        new BaseCellData(13, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 104
        new BaseCellData(17, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 105
        new BaseCellData(16, 1, 0, 0, false, new int[] { 0, 0 }),    // base cell 106
        new BaseCellData(9, 2, 0, 0, true, new int[] { 14, 18 }),   // base cell 107
        new BaseCellData(15, 1, 0, 1, false, new int[] { 0, 0 }),    // base cell 108
        new BaseCellData(15, 1, 0, 0, false, new int[] { 0, 0 }),    // base cell 109
        new BaseCellData(18, 0, 1, 1, false, new int[] { 0, 0 }),    // base cell 110
        new BaseCellData(18, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 111
        new BaseCellData(19, 0, 0, 1, false, new int[] { 0, 0 }),    // base cell 112
        new BaseCellData(17, 1, 0, 0, false, new int[] { 0, 0 }),    // base cell 113
        new BaseCellData(19, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 114
        new BaseCellData(18, 0, 1, 0, false, new int[] { 0, 0 }),    // base cell 115
        new BaseCellData(18, 1, 0, 1, false, new int[] { 0, 0 }),    // base cell 116
        new BaseCellData(19, 2, 0, 0, true, new int[] { -1, -1 }),  // base cell 117
        new BaseCellData(19, 1, 0, 0, false, new int[] { 0, 0 }),    // base cell 118
        new BaseCellData(18, 0, 0, 0, false, new int[] { 0, 0 }),    // base cell 119
        new BaseCellData(19, 1, 0, 1, false, new int[] { 0, 0 }),    // base cell 120
        new BaseCellData(18, 1, 0, 0, false, new int[] { 0, 0 })     // base cell 121
    };

    /**
     *  Return whether or not the indicated base cell is a pentagon.
     */
    public static boolean isBaseCellPentagon(int baseCell) {
        if (baseCell < 0 || baseCell >= Constants.NUM_BASE_CELLS) {  // LCOV_EXCL_BR_LINE
            // Base cells less than zero can not be represented in an index
            return false;
        }
        return baseCellData[baseCell].isPentagon;
    }

    /**
     *  Return whether or not the indicated base cell is a pentagon.
     */
    public static FaceIJK getBaseFaceIJK(int baseCell) {
        if (baseCell < 0 || baseCell >= Constants.NUM_BASE_CELLS) {  // LCOV_EXCL_BR_LINE
            // Base cells less than zero can not be represented in an index
            throw new IllegalArgumentException("Illegal base cell");
        }
        BaseCellData cellData = baseCellData[baseCell];
        return new FaceIJK(cellData.homeFace, new CoordIJK(cellData.homeI, cellData.homeJ, cellData.homeK));
    }

}
