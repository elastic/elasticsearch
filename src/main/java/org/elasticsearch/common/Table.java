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

package org.elasticsearch.common;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticSearchIllegalArgumentException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class Table {

    protected List<Cell> headers = new ArrayList<Cell>();
    protected List<List<Cell>> rows = new ArrayList<List<Cell>>();

    protected List<Cell> currentCells;

    protected boolean inHeaders = false;

    public Table startHeaders() {
        inHeaders = true;
        currentCells = new ArrayList<Cell>();
        return this;
    }

    public Table endHeaders() {
        inHeaders = false;
        headers = currentCells;
        currentCells = null;
        return this;
    }

    public Table startRow() {
        if (headers.isEmpty()) {
            throw new ElasticSearchIllegalArgumentException("no headers added...");
        }
        currentCells = new ArrayList<Cell>(headers.size());
        return this;
    }

    public Table endRow() {
        if (currentCells.size() != headers.size()) {
            throw new ElasticSearchIllegalArgumentException("mismatch on number of cells in a row compared to header");
        }
        rows.add(currentCells);
        currentCells = null;
        return this;
    }

    public Table addCell(Object value) {
        return addCell(value, "");
    }

    public Table addCell(Object value, String attributes) {
        if (!inHeaders) {
            if (currentCells.size() == headers.size()) {
                throw new ElasticSearchIllegalArgumentException("can't add more cells to a row than the header");
            }
        }
        Map<String, String> mAttr;
        if (attributes.length() == 0) {
            if (inHeaders) {
                mAttr = ImmutableMap.of();
            } else {
                // get the attributes of the header cell we are going to add to
                mAttr = headers.get(currentCells.size()).attr;
            }
        } else {
            mAttr = new HashMap<String, String>();
            if (!inHeaders) {
                // get the attributes of the header cell we are going to add
                mAttr.putAll(headers.get(currentCells.size()).attr);
            }
            String[] sAttrs = Strings.split(attributes, ";");
            for (String sAttr : sAttrs) {
                if (sAttr.length() == 0) {
                    continue;
                }
                int idx = sAttr.indexOf(':');
                mAttr.put(sAttr.substring(0, idx), sAttr.substring(idx + 1));
            }
        }
        currentCells.add(new Cell(value, mAttr));
        return this;
    }

    public List<Cell> getHeaders() {
        return this.headers;
    }

    public Iterable<List<Cell>> getRows() {
        return rows;
    }

    public static class Cell {
        public final Object value;
        public final Map<String, String> attr;

        public Cell(Object value) {
            this.value = value;
            this.attr = new HashMap<String, String>();
        }

        public Cell(Object value, Map<String, String> attr) {
            this.value = value;
            this.attr = attr;
        }
    }
}
