/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataTypes;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class ShowSession extends Command {

    private final String key, pattern;

    public ShowSession(Location location, String key, String pattern) {
        super(location);
        this.key = key;
        this.pattern = pattern;
    }

    public String key() {
        return key;
    }

    public String pattern() {
        return pattern;
    }

    @Override
    public List<Attribute> output() {
        return asList(new RootFieldAttribute(location(), "key", DataTypes.KEYWORD), 
                      new RootFieldAttribute(location(), "value", DataTypes.KEYWORD));
    }

    @Override
    protected RowSetCursor execute(SqlSession session) {
        List<List<?>> out;
        
        Settings s = session.settings().cfg();

        if (key != null) {
            out = singletonList(asList(key, s.get(key)));
        }
        else {
            if (pattern != null) {
                Pattern p = Pattern.compile(pattern);
                s = s.filter(k -> p.matcher(k).matches());
            }
            
            out = s.getAsMap().entrySet().stream()
                   .map(e -> asList(e.getKey(), e.getValue()))
                   .collect(toList()); 
        }

        return Rows.of(output(), out);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return true;
    }
}
