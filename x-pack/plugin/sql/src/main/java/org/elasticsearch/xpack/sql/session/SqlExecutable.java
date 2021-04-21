/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.session.Cursor.Page;

public interface SqlExecutable extends Executable {

    void execute(SqlSession session, ActionListener<Page> listener);

    @Override
    default void execute(Session session, ActionListener<Page> listener) {
        execute((SqlSession) session, listener);
    }
}
