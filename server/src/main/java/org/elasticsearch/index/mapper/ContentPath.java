/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

public final class ContentPath {

    private static final char DELIMITER = '.';

    private final StringBuilder sb;

    private int index = 0;

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

    public String remove() {
        var ret = path[--index];
        path[index] = null;
        return ret;
    }

    public void setWithinLeafObject(boolean withinLeafObject) {
        this.withinLeafObject = withinLeafObject;
    }

    public boolean isWithinLeafObject() {
        return withinLeafObject;
    }

    public String pathAsText(String name) {
        sb.setLength(0);
        for (int i = 0; i < index; i++) {
            sb.append(path[i]).append(DELIMITER);
        }
        sb.append(name);
        return sb.toString();
    }

    public int length() {
        return index;
    }
}
