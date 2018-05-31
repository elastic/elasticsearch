/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.rest.action.cat;

import org.elasticsearch.common.Table;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.rest.RestRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CatResponseTable extends Table {

    private boolean isValidRow;
    private List<Tuple<Object, String>> currentCells;
    private Matcher matcher;
    private RestRequest request;
    private boolean filterInverted;

    public CatResponseTable(RestRequest request) {
        this.request = request;
        this.filterInverted = false;
    }

    public CatResponseTable(RestRequest request, boolean inverted) {
        this.request = request;
        this.filterInverted = inverted;
    }

    public CatResponseTable setFilter(String pattern) {
        if (!pattern.isEmpty()) {
            this.matcher = Pattern.compile(pattern).matcher("");
        }
        return this;
    }

    @Override
    public CatResponseTable startRow() {
        isValidRow = filterInverted;
        currentCells = new ArrayList<>();
        return this;
    }

    @Override
    public CatResponseTable endRow() {
        if (currentCells == null) {
            throw new IllegalStateException("no row started...");
        }
        if (matcher == null || isValidRow) {
            super.startRow();
            for (Tuple<Object, String> t: currentCells) {
                super.addCell(t.v1(), t.v2());
            }
            super.endRow();
        } // otherwise skip this whole row
        currentCells = null;
        return this;
    }

    @Override
    public CatResponseTable addCell(Object value) {
        addCell(value, "");
        return this;
    }

    @Override
    public CatResponseTable addCell(Object value, String attributes) {
        if (super.isInHeaders()) {
            super.addCell(value, attributes);
        } else {
            if (currentCells == null) {
                throw new IllegalStateException("no row started...");
            }
            if (matcher != null) {
                matcher.reset(RestTable.renderValue(request, value));
                if (matcher.find()) {
                    isValidRow = !filterInverted;
                }
            }
            currentCells.add(new Tuple<>(value, attributes));
        }
        return this;
    }

}
