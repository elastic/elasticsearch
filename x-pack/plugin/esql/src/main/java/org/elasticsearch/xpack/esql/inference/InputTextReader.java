/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Helper class that reads text strings from a {@link BytesRefBlock}.
 * This class is used by inference operators to extract text content from block data.
 */
public class InputTextReader implements Releasable {
    private final BytesRefBlock textBlock;
    private final StringBuilder strBuilder = new StringBuilder();
    private BytesRef readBuffer = new BytesRef();

    public InputTextReader(BytesRefBlock textBlock) {
        this.textBlock = textBlock;
    }

    /**
     * Reads the text string at the given position.
     * Multiple values at the position are concatenated with newlines.
     *
     * @param pos the position index in the block
     * @return the text string at the position, or null if the position contains a null value
     */
    public String readText(int pos) {
        return readText(pos, Integer.MAX_VALUE);
    }

    /**
     * Reads the text string at the given position.
     *
     * @param pos   the position index in the block
     * @param limit the maximum number of value to read from the position
     * @return the text string at the position, or null if the position contains a null value
     */
    public String readText(int pos, int limit) {
        if (textBlock.isNull(pos)) {
            return null;
        }

        strBuilder.setLength(0);
        int maxPos = Math.min(limit, textBlock.getValueCount(pos));
        for (int valueIndex = 0; valueIndex < maxPos; valueIndex++) {
            readBuffer = textBlock.getBytesRef(textBlock.getFirstValueIndex(pos) + valueIndex, readBuffer);
            strBuilder.append(readBuffer.utf8ToString());
            if (valueIndex != maxPos - 1) {
                strBuilder.append("\n");
            }
        }

        return strBuilder.toString();
    }

    /**
     * Returns the total number of positions (text entries) in the block.
     */
    public int estimatedSize() {
        return textBlock.getPositionCount();
    }

    @Override
    public void close() {
        textBlock.allowPassingToDifferentDriver();
        Releasables.close(textBlock);
    }
}
