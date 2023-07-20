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

    private int dottedFieldNameIndex = 0;

    private String[] dottedFieldName = new String[10];

    private boolean withinLeafObject = false;

    public ContentPath() {
        this.sb = new StringBuilder();
    }

    public void add(String name) {
        path[index++] = name;
        if (index == path.length) { // expand if needed
            expandPath();
        }
    }

    public void addDottedFieldName(String name) {
        dottedFieldName[dottedFieldNameIndex++] = name;
        if (dottedFieldNameIndex == dottedFieldName.length) {
            expandDottedFieldName();
        }
    }

    private void expandPath() {
        String[] newPath = new String[path.length + 10];
        System.arraycopy(path, 0, newPath, 0, path.length);
        path = newPath;
    }

    private void expandDottedFieldName() {
        String[] newDottedFieldName = new String[dottedFieldName.length + 10];
        System.arraycopy(dottedFieldName, 0, newDottedFieldName, 0, dottedFieldName.length);
        dottedFieldName = newDottedFieldName;
    }

    public void remove() {
        path[index--] = null;
    }

    public void removeDottedFieldName() {
        dottedFieldName[dottedFieldNameIndex--] = null;
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

    public String dottedFieldName(String name) {
        sb.setLength(0);
        for (int i = 0; i < dottedFieldNameIndex; i++) {
            sb.append(dottedFieldName[i]).append(DELIMITER);
        }
        sb.append(name);
        return sb.toString();
    }

    public boolean hasDottedFieldName() {
        return dottedFieldNameIndex > 0;
    }

    public int length() {
        return index;
    }
}
