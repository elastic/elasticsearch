/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShowCatalogs extends Command {

    public ShowCatalogs(Source source) {
        super(source);
    }

    @Override
    protected NodeInfo<ShowCatalogs> info() {
        return NodeInfo.create(this);
    }

    @Override
    public List<Attribute> output() {
        return Arrays.asList(keyword("name"), keyword("type"));
    }

    @Override
    public void execute(SqlSession session, ActionListener<Page> listener) {
        List<List<?>> rows = new ArrayList<>();
        rows.add(Arrays.asList(session.indexResolver().clusterName(), "local"));
        session.indexResolver().remoteClusters().forEach(x -> rows.add(Arrays.asList(x, "remote")));
        listener.onResponse(of(session, rows));
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
