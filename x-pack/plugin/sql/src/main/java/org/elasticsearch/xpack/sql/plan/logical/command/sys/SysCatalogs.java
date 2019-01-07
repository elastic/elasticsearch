/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * System command returning the catalogs (clusters) available.
 * Currently returns only the current cluster name.
 */
public class SysCatalogs extends Command {

    public SysCatalogs(Source source) {
        super(source);
    }

    @Override
    protected NodeInfo<SysCatalogs> info() {
        return NodeInfo.create(this);
    }

    @Override
    public List<Attribute> output() {
        return singletonList(keyword("TABLE_CAT"));
    }

    @Override
    public final void execute(SqlSession session, ActionListener<SchemaRowSet> listener) {
        String cluster = session.indexResolver().clusterName();
        listener.onResponse(Rows.of(output(), singletonList(singletonList(cluster))));
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