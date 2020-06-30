/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Dedicated collection for mapping a key to a list of sequences */
/** The list represents the sequence for each stage (based on its index) and is fixed in size */

class KeyToSequences {

    private final int listSize;
    private final Map<SequenceKey, List<SequenceFrame>> keyToSequences;

    KeyToSequences(int listSize) {
        this.listSize = listSize;
        this.keyToSequences = new LinkedHashMap<>();
    }

    SequenceFrame frame(int stage, SequenceKey key) {
        return frame(key).get(stage);
    }

    private List<SequenceFrame> frame(SequenceKey key) {
        List<SequenceFrame> frames = keyToSequences.get(key);
        if (frames == null) {
            frames = new ArrayList<>(listSize);
            keyToSequences.put(key, frames);

            for (int i = 0; i < listSize; i++) {
                frames.add(new SequenceFrame());
            }
        }
        return frames;
    }

    SequenceFrame frameIfPresent(int stage, SequenceKey key) {
        List<SequenceFrame> list = keyToSequences.get(key);
        return list == null ? null : list.get(stage);
    }
}
