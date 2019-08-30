/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.util.Check;

import java.util.List;

import static java.util.Collections.emptyList;

public class SingletonExecutable implements Executable {

    private final List<Attribute> output;
    private final Object[] values;

    public SingletonExecutable() {
        this(emptyList());
    }

    public SingletonExecutable(List<Attribute> output, Object... values) {
        Check.isTrue(output.size() == values.length, "Attributes {} and values {} are out of sync", output, values);
        this.output = output;
        this.values = values;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public void execute(SqlSession session, ActionListener<Page> listener) {
        listener.onResponse(Page.last(Rows.singleton(output, values)));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            sb.append(output.get(i));
            sb.append("=");
            sb.append(values[i]);
        }
        return sb.toString();
    }
}
