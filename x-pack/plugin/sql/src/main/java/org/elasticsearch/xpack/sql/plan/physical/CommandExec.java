/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.List;
import java.util.Objects;

public class CommandExec extends LeafExec {

    private final Command command;

    public CommandExec(Source source, Command command) {
        super(source);
        this.command = command;
    }

    @Override
    protected NodeInfo<CommandExec> info() {
        return NodeInfo.create(this, CommandExec::new, command);
    }

    public Command command() {
        return command;
    }

    @Override
    public void execute(SqlSession session, ActionListener<Page> listener) {
        command.execute(session, listener.delegateFailureAndWrap((l, r) -> l.onResponse(r)));
    }

    @Override
    public List<Attribute> output() {
        return command.output();
    }

    @Override
    public int hashCode() {
        return Objects.hash(command);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CommandExec other = (CommandExec) obj;
        return Objects.equals(command, other.command);
    }
}
