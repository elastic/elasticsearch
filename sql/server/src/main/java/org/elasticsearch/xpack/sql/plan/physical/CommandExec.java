/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.List;
import java.util.Objects;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.SqlSession;

public class CommandExec extends LeafExec {

    private final Command command;

    public CommandExec(Command command) {
        super(command.location());
        this.command = command;
    }

    public Command command() {
        return command;
    }

    @Override
    public void execute(SqlSession session, ActionListener<RowSetCursor> listener) {
        command.execute(session, listener);
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