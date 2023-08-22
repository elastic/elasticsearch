/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

public final class ContentPath {

    private static final char DELIMITER = '.';

    private final StringBuilder sb;

    private int index = 0;
    private int sbIndex = 0;

    private String[] path = new String[10];

    private boolean withinLeafObject = false;

    public ContentPath() {
        this.sb = new StringBuilder();
    }

    String[] getPath() {
        // used for testing
        return path;
    }

    public void add(String name) {
        path[index++] = name;
        if (index == path.length) { // expand if needed
            expand();
        }
    }

    private void expand() {
        String[] newPath = new String[path.length + 10];
        System.arraycopy(path, 0, newPath, 0, path.length);
        path = newPath;
    }

    public void remove() {
        path[--index] = null;

        // Reset the StringBuilder if it includes a newly removed field
        if (index < sbIndex) {
            sbIndex = 0;
            sb.setLength(0);
        }
    }

    public void setWithinLeafObject(boolean withinLeafObject) {
        this.withinLeafObject = withinLeafObject;
    }

    public boolean isWithinLeafObject() {
        return withinLeafObject;
    }

    public String pathAsText(String name) {
        // If index is 0 we know that we are at the root, so return the provided string directly
        if (index == 0) {
            return name;
        }

        // Otherwise we preserve the previously built StringBuilder and append any necessary path fields
        while (sbIndex < index) {
            sb.append(path[sbIndex]).append(DELIMITER);
            sbIndex++;
        }

        return sb + name;
    }

    public int length() {
        return index;
    }
}
