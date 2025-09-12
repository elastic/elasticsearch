/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

public class PromptReader implements Releasable {
    private final BytesRefBlock promptBlock;
    private final StringBuilder strBuilder = new StringBuilder();
    private BytesRef readBuffer = new BytesRef();

    public PromptReader(BytesRefBlock promptBlock) {
        this.promptBlock = promptBlock;
    }

    /**
     * Reads the prompt string at the given position.
     *
     * @param pos the position index in the block
     */
    public String readPrompt(int pos) {
        if (promptBlock.isNull(pos)) {
            return null;
        }
        strBuilder.setLength(0);
        for (int valueIndex = 0; valueIndex < promptBlock.getValueCount(pos); valueIndex++) {
            readBuffer = promptBlock.getBytesRef(promptBlock.getFirstValueIndex(pos) + valueIndex, readBuffer);
            strBuilder.append(readBuffer.utf8ToString());
            if (valueIndex != promptBlock.getValueCount(pos) - 1) {
                strBuilder.append("\n");
            }
        }
        return strBuilder.toString();
    }

    /**
     * Returns the total number of positions (prompts) in the block.
     */
    public int estimatedSize() {
        return promptBlock.getPositionCount();
    }

    @Override
    public void close() {
        promptBlock.allowPassingToDifferentDriver();
        Releasables.close(promptBlock);
    }
}
