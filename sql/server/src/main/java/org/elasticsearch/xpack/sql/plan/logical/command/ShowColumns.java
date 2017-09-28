/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.session.RowSet;
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
    public void execute(SqlSession session, ActionListener<RowSet> listener) {
        session.getIndices(new String[]{index}, IndicesOptions.strictExpandOpenAndForbidClosed(), ActionListener.wrap(
                esIndices -> {
                    List<List<?>> rows = new ArrayList<>();
                    if (esIndices.isEmpty() == false) {
                        //TODO: we are using only the first index for now - add support for aliases
                        fillInRows(esIndices.get(0).mapping(), null, rows);
                    }
                    listener.onResponse(Rows.of(output(), rows));
                },
                listener::onFailure
        ));
    }

    @Override
    protected RowSet execute(SqlSession session) {
        throw new UnsupportedOperationException("No synchronous exec");
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