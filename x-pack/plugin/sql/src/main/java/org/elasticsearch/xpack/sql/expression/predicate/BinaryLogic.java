/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.AggNameInput;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

public abstract class BinaryLogic extends BinaryOperator {

    protected BinaryLogic(Location location, Expression left, Expression right, String symbol) {
        super(location, left, right, symbol);
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveInputType(DataType inputType) {
        return DataType.BOOLEAN == inputType ? TypeResolution.TYPE_RESOLVED : new TypeResolution(
                "'%s' requires type %s not %s", symbol(), DataType.BOOLEAN.sqlName(), inputType.sqlName());
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        return new ScriptTemplate("<not yet supported");
    }

    @Override
    protected Pipe makePipe() {
        return new AggNameInput(location(), this, "<not yet supported>");
    }
}
