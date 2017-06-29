/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import java.util.List;
import java.util.Objects;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataTypes;

import static java.util.Collections.singletonList;

public class SessionSet extends Command {

    private final String key, value;

    public SessionSet(Location location, String key, String value) {
        super(location);
        this.key = key;
        this.value = value;
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    @Override
    public List<Attribute> output() {
        return singletonList(new RootFieldAttribute(location(), "result", DataTypes.KEYWORD));

    }

    @Override
    protected RowSetCursor execute(SqlSession session) {
        session.updateSettings(s -> {
            return Settings.builder().put(s).put(key, value).build();
        });
        return Rows.empty(output());
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        SessionSet other = (SessionSet) obj;
        return Objects.equals(key, other.key) 
                && Objects.equals(value, other.value) ;
    }
}
