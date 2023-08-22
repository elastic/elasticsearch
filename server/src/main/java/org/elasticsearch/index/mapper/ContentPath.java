/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

/**
 * This class represents a content path that can be used to track elements and field names within a structure.
 * It provides methods for adding, removing, and generating paths and dotted field names.
 * The path and dotted field name is built using a delimiter to separate elements.
 */
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

    String[] getPath() {
        // used for testing
        return path;
    }

    String[] getDottedFieldName() {
        // used for testing
        return dottedFieldName;
    }

    /**
     * Adds a new element to the content path.
     *
     * @param name The name of the element to be added.
     */
    public void add(String name) {
        path[index++] = name;
        if (index == path.length) { // expand if needed
            expandPath();
        }
    }

    /**
     * Adds a new dotted field name to the content path.
     *
     * @param name The dotted field name to be added.
     */
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

    /**
     * Removes the last element from the content path and returns it.
     *
     * @return The removed element from the content path.
     */
    public String remove() {
        String removedPath = path[--index];
        path[index] = null;
        return removedPath;
    }

    /**
     * Removes the last dotted field name from the content path.
     */
    public void removeDottedFieldName() {
        dottedFieldName[--dottedFieldNameIndex] = null;
    }

    /**
     * Sets whether the current position is within a leaf object.
     *
     * @param withinLeafObject Whether the current position is within a leaf object.
     */
    public void setWithinLeafObject(boolean withinLeafObject) {
        this.withinLeafObject = withinLeafObject;
    }

    /**
     * Checks if the current position is within a leaf object.
     *
     * @return {@code true} if the current position is within a leaf object, {@code false} otherwise.
     */
    public boolean isWithinLeafObject() {
        return withinLeafObject;
    }

    /**
     * Generates the content path as text, appending the provided name.
     *
     * @param name The name to append to the content path.
     * @return The content path as text.
     */
    public String pathAsText(String name) {
        sb.setLength(0);
        for (int i = 0; i < index; i++) {
            sb.append(path[i]).append(DELIMITER);
        }
        sb.append(name);
        return sb.toString();
    }

    /**
     * Generates a dotted field name, appending the provided name.
     *
     * @param name The name to append to the dotted field name.
     * @return The dotted field name.
     */
    public String dottedFieldName(String name) {
        sb.setLength(0);
        for (int i = 0; i < dottedFieldNameIndex; i++) {
            sb.append(dottedFieldName[i]).append(DELIMITER);
        }
        sb.append(name);
        return sb.toString();
    }

    /**
     * Returns the number of elements in the content path.
     *
     * @return The length of the content path.
     */
    public int length() {
        return index;
    }

    /**
     * Returns the number of elements in the dotted field name.
     *
     * @return The length of the dotted field name.
     */
    public int dottedFieldNamelength() {
        return dottedFieldNameIndex;
    }
}
