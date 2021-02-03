/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.sql.util.Check;

public abstract class AbstractRowSet implements RowSet {
    private boolean terminated = false;

    @Override
    public Object column(int index) {
        Check.isTrue(index >= 0, "Invalid index {}; needs to be positive", index);
        Check.isTrue(index < columnCount(), "Invalid index {} for row of size {}", index, columnCount());
        Check.isTrue(hasCurrentRow(), "RowSet contains no (more) entries; use hasCurrent() to check its status");
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
        if (doNext() == false) {
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
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (hasCurrentRow()) {
            for (int column = 0; column < columnCount(); column++) {
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
