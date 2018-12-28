/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class ShowTables extends Command {

    private final String index;
    private final LikePattern pattern;

    public ShowTables(Source source, String index, LikePattern pattern) {
        super(source);
        this.index = index;
        this.pattern = pattern;
    }

    @Override
    protected NodeInfo<ShowTables> info() {
        return NodeInfo.create(this, ShowTables::new, index, pattern);
    }

    public String index() {
        return index;
    }

    public LikePattern pattern() {
        return pattern;
    }

    @Override
    public List<Attribute> output() {
        return asList(keyword("name"), keyword("type"));
    }

    @Override
    public final void execute(SqlSession session, ActionListener<SchemaRowSet> listener) {
        String idx = index != null ? index : (pattern != null ? pattern.asIndexNameWildcard() : "*");
        String regex = pattern != null ? pattern.asJavaRegex() : null;
        session.indexResolver().resolveNames(idx, regex, null, ActionListener.wrap(result -> {
            listener.onResponse(Rows.of(output(), result.stream()
                 .map(t -> asList(t.name(), t.type().toSql()))
                .collect(toList())));
        }, listener::onFailure));
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