/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.table;

import java.util.ArrayList;

/**
 *  A generic table renderer.  Can optionally print header row.
 *  Will justify all cells in a column to the widest one.  All rows need
 *  to have same number of cells.
 *
 *  Eg, new Table.addRow(new Row().addCell("foo").addCell("bar")).render()
 */
public class Table {
    private ArrayList<Column> cols;

    private byte numcols;

    private byte height;

    public Table() {
        this.cols = new ArrayList<Column>();
        this.numcols = 0;
        this.height = 0;
    }

    public Table addRow(Row row) {
        addRow(row, false);
        return this;
    }

    public void ensureCapacity(int size) {
        if (numcols < size) {
            for (int i = 0; i < (size - numcols); i++) {
                cols.add(new Column());
            }
        }
    }

    public Table addRow(Row row, boolean header) {
        ensureCapacity(row.size());
        byte curCol = 0;
        for (Cell cell : row.cells()) {
            Column col = cols.get(curCol);
            col.addCell(cell, header);
            curCol += 1;
        }
        numcols = curCol;
        height += 1;
        return this;
    }

    public String render() {
        return render(false);
    }

    public String render(boolean withHeaders) {
        StringBuilder out = new StringBuilder();
        for (byte i = 0; i < height; i++) {
            StringBuilder row = new StringBuilder();
            for (Column col : cols) {
                Cell cell = col.getCell(i);
                boolean headerRowWhenNotWantingHeaders = i == 0 && !withHeaders && col.hasHeader();
                if (! headerRowWhenNotWantingHeaders) {
                    row.append(cell.toString(col.width(), col.align()));
                    row.append(" ");
                }
            }
            out.append(row.toString().trim());
            out.append("\n");
        }
        return out.toString();
    }

    private class Column {
        private boolean hasHeader;

        private ArrayList<Cell> cells;

        private byte width;

        private Align align;

        Column () {
            cells = new ArrayList<Cell>();
            width = 0;
            hasHeader = false;
            align = Align.LEFT;
        }

        public Column addCell(Cell cell) {
            addCell(cell, false);
            return this;
        }

        public Column addCell(Cell cell, boolean header) {
            cells.add(cell);

            if (header) {
                hasHeader = true;
            }

            if (cell.width() > width) {
                width = cell.width();
            }

            if (align != cell.align()) {
                align = cell.align();
            }

            return this;
        }

        public Cell getCell(int index) {
            return cells.get(index);
        }

        public Align align() {
            return this.align;
        }

        public byte width() {
            return this.width;
        }

        public boolean hasHeader() {
            return this.hasHeader;
        }
    }
}
