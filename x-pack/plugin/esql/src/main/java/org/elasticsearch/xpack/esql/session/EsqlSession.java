/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

public class EsqlSession {

    public void execute(String query, ActionListener<Result> listener) {
        try {
            Executable plan = (Executable) parse(query);
            plan.execute(this, listener);
        } catch (ParsingException pe) {
            listener.onFailure(pe);
        }
    }

    private LogicalPlan parse(String query) {
        return new EsqlParser().createStatement(query);
    }

}
