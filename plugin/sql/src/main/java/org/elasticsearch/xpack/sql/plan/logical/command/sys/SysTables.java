/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.regex.LikePattern;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public class SysTables extends Command {

    private final LikePattern pattern;

    public SysTables(Location location, LikePattern pattern) {
        super(location);
        this.pattern = pattern;
    }

    @Override
    protected NodeInfo<SysTables> info() {
        return NodeInfo.create(this, SysTables::new, pattern);
    }

    public LikePattern pattern() {
        return pattern;
    }

    @Override
    public List<Attribute> output() {
        return asList(keyword("TABLE_CAT"), 
                      keyword("TABLE_SCHEM"),
                      keyword("TABLE_NAME"),
                      keyword("TABLE_TYPE"),
                      keyword("REMARKS"),
                      keyword("TYPE_CAT"),
                      keyword("TYPE_SCHEM"),
                      keyword("TYPE_NAME"),
                      keyword("SELF_REFERENCING_COL_NAME"),
                      keyword("REF_GENERATION")
                      );
    }

    @Override
    public final void execute(SqlSession session, ActionListener<SchemaRowSet> listener) {
        String index = pattern != null ? pattern.asIndexNameWildcard() : "*";
        String regex = pattern != null ? pattern.asJavaRegex() : null;

        String cluster = session.indexResolver().clusterName();
        session.indexResolver().resolveNames(index, regex, ActionListener.wrap(result -> listener.onResponse(
                Rows.of(output(), result.stream()
                 .map(t -> asList(cluster,
                         EMPTY,
                         t.name(), 
                         t.type().toSql(),
                         EMPTY,
                         null,
                         null,
                         null,
                         null,
                         null))
                .collect(toList())))
        , listener::onFailure));
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SysTables other = (SysTables) obj;
        return Objects.equals(pattern, other.pattern);
    }
}