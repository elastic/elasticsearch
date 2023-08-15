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

    private String[] path = new String[10];

    private StringBuilder[] textPath = new StringBuilder[10];

    private boolean withinLeafObject = false;

    public ContentPath() {
        this.sb = new StringBuilder();
    }

    public void add(String name) {
        if(index == 0) {
            textPath[index] = new StringBuilder();
            textPath[index].append(name);
        } else {
            textPath[index] = new StringBuilder();
            textPath[index].append(textPath[index-1]).append(DELIMITER).append(name);
        }
        path[index++] = name;
        if (index == path.length) { // expand if needed
            expand();
        }
    }

    private void expand() {
        String[] newPath = new String[path.length + 10];
        StringBuilder[] newTextPath = new StringBuilder[path.length + 10];
        System.arraycopy(path, 0, newPath, 0, path.length);
        System.arraycopy(textPath, 0, newTextPath, 0, path.length);
        path = newPath;
        textPath = newTextPath;
    }

    public void remove() {
        textPath[index] = new StringBuilder();
        path[index--] = null;
    }

    public void setWithinLeafObject(boolean withinLeafObject) {
        this.withinLeafObject = withinLeafObject;
    }

    public boolean isWithinLeafObject() {
        return withinLeafObject;
    }

    public String pathAsText(String name) {
        sb.setLength(0);
        if(index != 0){
            sb.append(textPath[index-1]);
        }
        if(sb.length() > 1) {
            sb.append(DELIMITER);
        }
        sb.append(name);
        return sb.toString();
    }

    public int length() {
        return index;
    }
}
