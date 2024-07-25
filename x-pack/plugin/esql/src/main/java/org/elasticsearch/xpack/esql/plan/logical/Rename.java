/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveRefs;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;

import java.util.List;
import java.util.Objects;

public class Rename extends UnaryPlan {

    private final List<Alias> renamings;

    public Rename(Source source, LogicalPlan child, List<Alias> renamings) {
        super(source, child);
        this.renamings = renamings;
    }

    public List<Alias> renamings() {
        return renamings;
    }

    @Override
    public List<Attribute> output() {
        // Normally shouldn't reach here, as Rename only exists before resolution.
        List<NamedExpression> projectionsAfterResolution = ResolveRefs.projectionsForRename(this, this.child().output(), null);

        return Expressions.asAttributes(projectionsAfterResolution);
    }

    @Override
    public boolean expressionsResolved() {
        for (var alias : renamings) {
            // don't call dataType() - it will fail on UnresolvedAttribute
            if (alias.resolved() == false && alias.child() instanceof UnsupportedAttribute == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Rename(source(), newChild, renamings);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Rename::new, child(), renamings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), renamings);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        return Objects.equals(renamings, ((Rename) obj).renamings);
    }
}
