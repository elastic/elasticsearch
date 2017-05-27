/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.xpack.sql.analysis.catalog.EsType;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataTypes;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class ShowTables extends Command {

    private final String index, pattern;

    public ShowTables(Location location, String index, String pattern) {
        super(location);
        this.index = index;
        this.pattern = pattern;
    }

    public String index() {
        return index;
    }

    public String pattern() {
        return pattern;
    }

    @Override
    public List<Attribute> output() {
        return asList(new RootFieldAttribute(location(), "index", DataTypes.KEYWORD),
                      new RootFieldAttribute(location(), "type", DataTypes.KEYWORD));
    }

    @Override
    protected RowSetCursor execute(SqlSession session) {
        Collection<EsType> types = session.catalog().listTypes(index, pattern);
        
        return Rows.of(output(), types.stream()
                .map(t -> asList(t.index(), t.name()))
                .collect(toList()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        ShowTables other = (ShowTables) obj;
        return Objects.equals(index, other.index)
                && Objects.equals(pattern, other.pattern);
    }
}
