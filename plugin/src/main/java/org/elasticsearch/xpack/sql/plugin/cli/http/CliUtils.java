/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli.http;

import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.xpack.sql.session.RowSetCursor;

public abstract class CliUtils { // TODO made public so it could be shared with tests

    // this toString is a bit convoluted since it tries to be smart and pad the columns according to their values
    // as such it will look inside the row, find the max for each column and pad all the values accordingly
    // things are more complicated when the value is split across multiple lines (like a plan) in which case
    // a row needs to be iterated upon to fill up the values that don't take extra lines

    // Warning: this method _consumes_ a rowset
    public static String toString(RowSetCursor cursor) {
        if (cursor.rowSize() == 1 && cursor.size() == 1 && cursor.column(0).toString().startsWith("digraph ")) {
            return cursor.column(0).toString();
        }

        StringBuilder sb = new StringBuilder();

        // use the schema as a header followed by
        // do a best effort to compute the width of the column
        int[] width = new int[cursor.rowSize()];

        for (int i = 0; i < cursor.rowSize(); i++) {
            // TODO: default schema width
            width[i] = Math.max(15, cursor.schema().names().get(i).length());
        }

        AtomicBoolean firstRun = new AtomicBoolean(true);
        // take a look at the first values
        cursor.forEachSet(rowSet -> {
            for (boolean shouldRun = rowSet.hasCurrent(); shouldRun; shouldRun = rowSet.advance()) {
                for (int column = 0; column < rowSet.rowSize(); column++) {
                    if (column > 0) {
                        sb.append("|");
                    }

                    String val = String.valueOf(rowSet.column(column));
                    // the value might contain multiple lines (plan execution for example)
                    // TODO: this needs to be improved to properly scale each row across multiple lines
                    String[] split = val.split("\\n");

                    if (firstRun.get()) {
                        // find max across splits
                        for (int splitIndex = 0; splitIndex < split.length; splitIndex++) {
                            width[column] = Math.max(width[column], split[splitIndex].length());
                        }
                    }

                    for (int splitIndex = 0; splitIndex < split.length; splitIndex++) {
                        if (splitIndex > 0) {
                            sb.append("\n");
                        }
                        String string = split[splitIndex];

                        // does the value fit the column ?
                        if (string.length() <= width[column]) {
                            sb.append(string);
                            // pad value
                            for (int k = 0; k < width[column] - string.length(); k++) {
                                sb.append(" ");
                            }
                        }
                        // no, then trim it
                        else {
                            sb.append(string.substring(0, width[column] - 1));
                            sb.append("~");
                        }
                    }
                }

                sb.append("\n");
                firstRun.set(false);
            }
        });

        // compute the header
        StringBuilder header = new StringBuilder();

        for (int i = 0; i < cursor.rowSize(); i++) {
            if (i > 0) {
                header.append("|");
            }

            String name = cursor.schema().names().get(i);
            // left padding
            int leftPadding = (width[i] - name.length()) / 2;
            for (int j = 0; j < leftPadding; j++) {
                header.append(" ");
            }
            header.append(name);
            // right padding
            for (int j = 0; j < width[i] - name.length() - leftPadding; j++) {
                header.append(" ");
            }
        }

        header.append("\n");
        for (int i = 0; i < cursor.rowSize(); i++) {
            if (i > 0) {
                header.append("+");
            }
            for (int j = 0; j < width[i]; j++) {
                header.append("-"); // emdash creates issues
            }
        }

        header.append("\n");

        // append the header
        sb.insert(0, header.toString());

        return sb.toString();
    }
}