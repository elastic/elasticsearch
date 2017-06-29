/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;


import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnsProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.util.StringUtils;

public class Pi extends MathFunction {

    public Pi(Location location) {
        super(location);
    }

    public boolean foldable() {
        return true;
    }

    @Override
    public Object fold() {
        return Math.PI;
    }

    @Override
    public ColumnsProcessor asProcessor() {
        return l -> Math.PI;
    }

    @Override
    protected Object math(double d) {
        throw new SqlIllegalArgumentException("unused");
    }

    @Override
    protected ScriptTemplate asScript() {
        return new ScriptTemplate(StringUtils.EMPTY);
    }
}
