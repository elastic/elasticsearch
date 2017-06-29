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
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataTypes;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class SessionReset extends Command {

    private final String key, pattern;

    public SessionReset(Location location, String key, String pattern) {
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
        session.updateSettings(s -> {
            Settings defaults = session.defaults().cfg();
            Builder builder = Settings.builder().put(s);
            if (pattern != null) {
                Pattern p = Pattern.compile(pattern);
                s.getAsMap().forEach((k, v) -> {
                    if (p.matcher(k).matches()) {
                        builder.put(k, defaults.get(k));
                    }
                });
            }
            else {
                builder.put(key, defaults.get(key));
            }
            return builder.build();
        });
        
        return Rows.of(output(), session.settings().cfg().getAsMap().entrySet().stream()
                .map(e -> asList(e.getKey(), e.getValue()))
                .collect(toList()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, pattern);
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        SessionReset other = (SessionReset) obj;
        return Objects.equals(key, other.key) 
                && Objects.equals(pattern, other.pattern) ;
    }
}