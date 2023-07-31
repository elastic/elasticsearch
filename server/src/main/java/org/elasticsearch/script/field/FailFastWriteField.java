/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class FailFastWriteField extends WriteField {

    private Supplier<IllegalArgumentException> resolutionError;

    public FailFastWriteField(String path, Supplier<Map<String, Object>> rootSupplier) {
        super(path, rootSupplier);
    }

    public Object get() {
        Object value = get(MISSING);
        throwResolutionErrorIfPresent();
        if (value == MISSING) {
            throw new IllegalArgumentException("cannot resolve path [" + path + "]");
        }
        return value;
    }

    @Override
    protected void resolveDepthFlat() {
        clearResolutionErrors();
        super.resolveDepthFlat();
    }

    @Override
    protected void createDepth() {
        super.createDepth();
        clearResolutionErrors();
    }

    protected void onResolutionSuccess(String leaf) {
        super.onResolutionSuccess(leaf);
        clearResolutionErrors();
    }

    @Override
    protected void setPath(String path) {
        super.setPath(path);
        clearResolutionErrors();
    }

    @Override
    protected void setToContainer(Object value) {
        super.setToContainer(value);
        clearResolutionErrors();
    }

    @Override
    public void remove() {
        throwResolutionErrorIfPresent();
        super.remove();
    }

    @Override
    public boolean exists(boolean failOutOfRange) {
        return super.exists(failOutOfRange);
    }

    protected void onResolutionErrorNotFound(int lastIndex) {
        super.onResolutionErrorNotFound(lastIndex);
        if (resolutionError == null) {
            resolutionError = () -> new IllegalArgumentException(
                "field [" + path.substring(path.indexOf('.', lastIndex) + 1) + "] not present as part of path [" + path + "]"
            );
        }
    }

    protected void onResolutionErrorIndexNotANumber(String leaf, NumberFormatException e) {
        if (resolutionError == null) {
            resolutionError = () -> new IllegalArgumentException(
                "[" + leaf + "] is not an integer, cannot be used as an index as part of path [" + path + "]",
                e
            );
        }
    }

    protected void onResolutionErrorWrongType(String leaf, Object container) {
        if (resolutionError == null) {
            resolutionError = () -> new IllegalArgumentException(
                "cannot resolve [" + leaf + "] from object of type [" + typeName(container) + "] as part of path [" + path + "]"
            );
        }
    }

    protected void onResolutionErrorMissingContainer(String leaf) {
        if (resolutionError == null) {
            resolutionError = () -> new IllegalArgumentException("cannot resolve [" + leaf + "] from null as part of path [" + path + "]");
        }
    }

    protected void onResolutionErrorOutOfBounds(List<?> listContainer, int index) {
        if (resolutionError == null) {
            resolutionError = () -> new IllegalArgumentException(
                "[" + index + "] is out of bounds for array with length [" + listContainer.size() + "] as part of path [" + path + "]"
            );
        }
    }

    protected void onResolutionErrorMissingKey(String leaf) {
        if (resolutionError == null) {
            resolutionError = () -> new IllegalArgumentException("field [" + leaf + "] not present as part of path [" + path + "]");
        }
    }

    private void clearResolutionErrors() {
        resolutionError = null;
    }

    private void throwResolutionErrorIfPresent() {
        if (resolutionError != null) {
            throw resolutionError.get();
        }
    }

    protected void onModificationIndexNotANumber(String leaf, NumberFormatException e) {
        throw new IllegalArgumentException(
            "[" + leaf + "] is not an integer, cannot be used as an index as part of path [" + path + "]",
            e
        );
    }

    @Override
    protected void onRemoveOutOfBounds(List<?> listContainer, int index) {
        onModificationOutOfBounds(listContainer, index);
    }

    @Override
    protected void onSetOutOfBounds(Object value, List<Object> listContainer, int index) {
        onModificationOutOfBounds(listContainer, index);
    }

    private void onModificationOutOfBounds(List<?> listContainer, int index) {
        throw new IllegalArgumentException(
            "[" + index + "] is out of bounds for array with length [" + listContainer.size() + "] as part of path [" + path + "]"
        );
    }
}
