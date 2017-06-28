/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import java.util.function.Consumer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.type.Schema;
import org.elasticsearch.xpack.sql.util.Assert;

public abstract class AbstractRowSetCursor implements RowSetCursor {

    private final Schema schema;
    private final int size;

    private final Consumer<ActionListener<RowSetCursor>> nextSet;

    private boolean terminated = false;

    protected AbstractRowSetCursor(Schema schema, Consumer<ActionListener<RowSetCursor>> nextSet) {
        this.schema = schema;
        this.size = schema().names().size();
        this.nextSet = nextSet;
    }

    @Override
    public Object column(int index) {
        Assert.isTrue(index >= 0, "Invalid index %d; needs to be positive", index);
        Assert.isTrue(index < rowSize(), "Invalid index %d for row of size %d", index, rowSize());
        Assert.isTrue(hasCurrentRow(), "RowSet contains no (more) entries; use hasCurrent() to check its status");
        return getColumn(index);
    }

    protected abstract Object getColumn(int column);

    @Override
    public boolean hasCurrentRow() {
        return terminated ? false : doHasCurrent();
    }

    @Override
    public boolean advanceRow() {
        if (terminated) {
            return false;
        }
        if (!doNext()) {
            terminated = true;
            return false;
        }
        return true;
    }

    protected abstract boolean doHasCurrent();

    protected abstract boolean doNext();

    @Override
    public void reset() {
        terminated = false;
        doReset();
    }

    protected abstract void doReset();

    @Override
    public boolean hasNextSet() {
        return nextSet != null;
    }

    @Override
    public void nextSet(ActionListener<RowSetCursor> listener) {
        if (nextSet != null) {
            nextSet.accept(listener);
        }
        else {
            listener.onResponse(null);
        }
    }

    @Override
    public int rowSize() {
        return size;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (hasCurrentRow()) {
            for (int column = 0; column < size; column++) {
                if (column > 0) {
                    sb.append("|");
                }

                String val = String.valueOf(getColumn(column));
                // the value might contain multiple lines (plan execution for example)
                // TODO: this needs to be improved to properly scale each row across multiple lines
                String[] split = val.split("\\n");
                
                for (int splitIndex = 0; splitIndex < split.length; splitIndex++) {
                    if (splitIndex > 0) {
                        sb.append("\n");
                    }
                    String string = split[splitIndex];
                    sb.append(string);
                }
            }
            sb.append("\n");
        }

        return sb.toString();
    }
}