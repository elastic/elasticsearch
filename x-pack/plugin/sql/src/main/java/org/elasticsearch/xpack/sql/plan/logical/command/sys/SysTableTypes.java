/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver.IndexType;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Comparator;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * System command returning the types of tables supported,
 * index and alias.
 */
public class SysTableTypes extends Command {

    public SysTableTypes(Source source) {
        super(source);
    }

    @Override
    protected NodeInfo<SysTableTypes> info() {
        return NodeInfo.create(this);
    }

    @Override
    public List<Attribute> output() {
        return singletonList(keyword("TABLE_TYPE"));
    }

    @Override
    public final void execute(SqlSession session, ActionListener<SchemaRowSet> listener) {
        listener.onResponse(Rows.of(output(), IndexType.VALID.stream()
                // *DBC requires ascending order
                .sorted(Comparator.comparing(t -> t.toSql()))
                .map(t -> singletonList(t.toSql()))
                .collect(toList())));
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
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