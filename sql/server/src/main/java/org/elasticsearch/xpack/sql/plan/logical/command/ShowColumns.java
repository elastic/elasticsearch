/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
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
import static java.util.Collections.emptyList;

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
        return asList(new FieldAttribute(location(), "column", DataTypes.KEYWORD),
                new FieldAttribute(location(), "type", DataTypes.KEYWORD));
    }

    @Override
    public void execute(SqlSession session, ActionListener<SchemaRowSet> listener) {
        session.indexResolver().resolveWithSameMapping(index, null, ActionListener.wrap(
                indexResult -> {
                    List<List<?>> rows = emptyList();
                    if (indexResult.isValid()) {
                        rows = new ArrayList<>();
                        fillInRows(indexResult.get().mapping(), null, rows);
                    }
                    listener.onResponse(Rows.of(output(), rows));
                },
                listener::onFailure
                ));
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