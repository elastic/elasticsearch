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

    private final int offset;

    private int index = 0;

    private String[] path = new String[10];

    public ContentPath() {
        this(0);
    }

    /**
     * Constructs a json path with an offset. The offset will result an {@code offset}
     * number of path elements to not be included in {@link #pathAsText(String)}.
     */
    public ContentPath(int offset) {
        this.sb = new StringBuilder();
        this.offset = offset;
        this.index = 0;
    }

    public ContentPath(String path) {
        this.sb = new StringBuilder();
        this.offset = 0;
        this.index = 0;
        add(path);
    }

    public void add(String name) {
        path[index++] = name;
        if (index == path.length) { // expand if needed
            String[] newPath = new String[path.length + 10];
            System.arraycopy(path, 0, newPath, 0, path.length);
            path = newPath;
        }
    }

    public void remove() {
        path[index--] = null;
    }

    public String pathAsText(String name) {
        sb.setLength(0);
        for (int i = offset; i < index; i++) {
            sb.append(path[i]).append(DELIMITER);
        }
        sb.append(name);
        return sb.toString();
    }

    public int length() {
        return index;
    }

    public boolean atRoot() {
        return index == 0;
    }
}
