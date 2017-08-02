/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.CompoundDataType;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.util.Arrays.asList;

public class ShowColumns extends Command {

    private final String index;

    public ShowColumns(Location location, String index) {
        super(location);
        this.index = index;
    }

    public String index() {
        return index;
    }

    @Override
    public List<Attribute> output() {
        return asList(new RootFieldAttribute(location(), "column", DataTypes.KEYWORD), 
                      new RootFieldAttribute(location(), "type", DataTypes.KEYWORD));
    }

    @Override
    protected RowSetCursor execute(SqlSession session) {
        Catalog catalog = session.catalog();
        Map<String, DataType> mapping = catalog.getIndex(index).mapping();
        
        List<List<?>> rows = new ArrayList<>();
        fillInRows(mapping, null, rows);
        return Rows.of(output(), rows);
    }

    private void fillInRows(Map<String, DataType> mapping, String prefix, List<List<?>> rows) {
        for (Entry<String, DataType> e : mapping.entrySet()) {
            DataType dt = e.getValue();
            String name = e.getKey();
            if (dt != null) {
                rows.add(asList(prefix != null ? prefix + "." + name : name, dt.sqlName()));
                if (dt instanceof CompoundDataType) {
                    String newPrefix = prefix != null ? prefix + "." + name : name;
                    fillInRows(((CompoundDataType) dt).properties(), newPrefix, rows);
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        ShowColumns other = (ShowColumns) obj;
        return Objects.equals(index, other.index);
    }
}