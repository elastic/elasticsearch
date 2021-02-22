/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.proto;

/**
 * Utilities for working with geometry command headers.
 *
 * Forked from com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr.java
 *
 */
public final class GeomCmdHdr {

    private static int CLOSE_PATH_HDR = cmdHdr(GeomCmd.ClosePath, 1);

    /**
     * <p>Encodes a 'command header' with the first 3 LSB as the command id, the remaining bits
     * as the command length. See the vector-tile-spec for details.</p>
     *
     * @param cmd command to execute
     * @param length how many times the command is repeated
     * @return encoded 'command header' integer
     */
    public static int cmdHdr(GeomCmd cmd, int length) {
        return (cmd.getCmdId() & 0x7) | (length << 3);
    }

    /**
     * Get the length component from the 'command header' integer.
     *
     * @param cmdHdr encoded 'command header' integer
     * @return command length
     */
    public static int getCmdLength(int cmdHdr) {
        return cmdHdr >> 3;
    }

    /**
     * Get the id component from the 'command header' integer.
     *
     * @param cmdHdr encoded 'command header' integer
     * @return command id
     */
    public static int getCmdId(int cmdHdr) {
        return cmdHdr & 0x7;
    }

    /**
     * Get the id component from the 'command header' integer, then find the
     * {@link GeomCmd} with a matching id.
     *
     * @param cmdHdr encoded 'command header' integer
     * @return command with matching id, or null if a match could not be made
     */
    public static GeomCmd getCmd(int cmdHdr) {
        final int cmdId = getCmdId(cmdHdr);
        return GeomCmd.fromId(cmdId);
    }

    /**
     * @return encoded 'command header' integer for {@link GeomCmd#ClosePath}.
     */
    public static int closePathCmdHdr() {
        return CLOSE_PATH_HDR;
    }

    /**
     * Maximum allowed 'command header' length value.
     */
    public static final int CMD_HDR_LEN_MAX = (int) (Math.pow(2, 29) - 1);
}
