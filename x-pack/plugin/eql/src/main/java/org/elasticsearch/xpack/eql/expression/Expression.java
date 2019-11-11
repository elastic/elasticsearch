/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.tree.Node;
import org.elasticsearch.xpack.eql.tree.Source;
import org.elasticsearch.xpack.eql.type.DataType;

import java.util.List;

public abstract class Expression extends Node<Expression> {

    /**
     * Order is important in the enum; any values should be added at the end.
     */
    public enum Stage {
        PARSED, PRE_ANALYZED, ANALYZED, OPTIMIZED;
    }

    private Stage stage = Stage.PARSED;

    public Expression(Source source, List<Expression> children) {
        super(source, children);
    }

    public boolean preAnalyzed() {
        return stage.ordinal() >= Stage.PRE_ANALYZED.ordinal();
    }

    public void setPreAnalyzed() {
        stage = Stage.PRE_ANALYZED;
    }

    public boolean analyzed() {
        return stage.ordinal() >= Stage.ANALYZED.ordinal();
    }

    public void setAnalyzed() {
        stage = Stage.ANALYZED;
    }

    public boolean optimized() {
        return stage.ordinal() >= Stage.OPTIMIZED.ordinal();
    }

    public void setOptimized() {
        stage = Stage.OPTIMIZED;
    }

    public boolean foldable() {
        return false;
    }

    public Object fold() {
        throw new EqlIllegalArgumentException("{} is not foldable", toString());
    }

    public abstract DataType dataType();

    @Override
    public String toString() {
        return sourceText();
    }
}