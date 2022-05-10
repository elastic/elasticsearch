/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;

public final class ContentPath {

    private static final char DELIMITER = '.';

    private static final ThreadLocal<StringBuilder> stringBuilder = ThreadLocal.withInitial(StringBuilder::new);

    private int index;

    private String[] path = Strings.EMPTY_ARRAY;

    public ContentPath() {
        this.index = 0;
    }

    public ContentPath(String path) {
        this.index = 0;
        add(path);
    }

    public void add(String name) {
        if (index == path.length) { // expand if needed
            String[] newPath = new String[path.length + 10];
            System.arraycopy(path, 0, newPath, 0, path.length);
            path = newPath;
        }
        path[index++] = name;
    }

    public void remove() {
        path[index--] = null;
    }

    public String pathAsText(String name) {
        if (index == 0) {
            return name;
        }
        final StringBuilder sb = stringBuilder.get();
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

    public boolean atRoot() {
        return index == 0;
    }
}
