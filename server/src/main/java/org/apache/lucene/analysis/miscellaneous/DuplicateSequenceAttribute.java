/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.util.Attribute;

/**
 * Provides statistics useful for detecting duplicate sections of text
 */
public interface DuplicateSequenceAttribute extends Attribute {
    /**
     * @return The number of times this token has been seen previously as part
     *         of a sequence (counts to a max of 255)
     */
    short getNumPriorUsesInASequence();

    void setNumPriorUsesInASequence(short len);
}
