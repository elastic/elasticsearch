/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Stack;

public final class ContentPath {

    private static final char DELIMITER = '.';

    private final StringBuilder sb;
    private final Stack<Integer> delimiterIndexes;
    private boolean withinLeafObject = false;

    public ContentPath() {
        this.sb = new StringBuilder();
        this.delimiterIndexes = new Stack<>();
    }

    public void add(String name) {
        // Store the location of the previous final delimiter onto the stack,
        // which will be the index of the 2nd last delimiter after appending the new name
        delimiterIndexes.add(sb.length() - 1);
        sb.append(name).append(DELIMITER);
    }

    public void remove() {
        if (delimiterIndexes.isEmpty()) {
            throw new IllegalStateException("Content path is empty");
        }

        // Deletes the last node added to the stringbuilder by deleting from the 2nd last delimiter onwards
        sb.setLength(delimiterIndexes.pop() + 1);
    }

    public void setWithinLeafObject(boolean withinLeafObject) {
        this.withinLeafObject = withinLeafObject;
    }

    public boolean isWithinLeafObject() {
        return withinLeafObject;
    }

    public String pathAsText(String name) {
        // If length is 0 we know that we are at the root, so return the provided string directly
        if (length() == 0) {
            return name;
        }

        return sb + name;
    }

    public int length() {
        // The amount of delimiters we've added tells us the amount of nodes that have been added to the path
        return delimiterIndexes.size();
    }
}
