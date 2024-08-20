/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

public final class DuplicateLimitAfterMvExpand extends OptimizerRules.OptimizerRule<Limit> {

    @Override
    protected LogicalPlan rule(Limit limit) {
        var child = limit.child();
        var shouldSkip = child instanceof Eval
            || child instanceof Project
            || child instanceof RegexExtract
            || child instanceof Enrich
            || child instanceof Limit;

        if (shouldSkip == false && child instanceof UnaryPlan unary) {
            MvExpand mvExpand = descendantMvExpand(unary);
            if (mvExpand != null) {
                Limit limitBeforeMvExpand = limitBeforeMvExpand(mvExpand);
                // if there is no "appropriate" limit before mv_expand, then push down a copy of the one after it so that:
                // - a possible TopN is properly built as low as possible in the tree (closed to Lucene)
                // - the input of mv_expand is as small as possible before it is expanded (less rows to inflate and occupy memory)
                if (limitBeforeMvExpand == null) {
                    var duplicateLimit = new Limit(limit.source(), limit.limit(), mvExpand.child());
                    return limit.replaceChild(propagateDuplicateLimitUntilMvExpand(duplicateLimit, mvExpand, unary));
                }
            }
        }
        return limit;
    }

    private static MvExpand descendantMvExpand(UnaryPlan unary) {
        UnaryPlan plan = unary;
        AttributeSet filterReferences = new AttributeSet();
        while (plan instanceof Aggregate == false) {
            if (plan instanceof MvExpand mve) {
                // don't return the mv_expand that has a filter after it which uses the expanded values
                // since this will trigger the use of a potentially incorrect (too restrictive) limit further down in the tree
                if (filterReferences.isEmpty() == false) {
                    if (filterReferences.contains(mve.target()) // the same field or reference attribute is used in mv_expand AND filter
                        || mve.target() instanceof ReferenceAttribute // or the mv_expand attr hasn't yet been resolved to a field attr
                        // or not all filter references have been resolved to field attributes
                        || filterReferences.stream().anyMatch(ref -> ref instanceof ReferenceAttribute)) {
                        return null;
                    }
                }
                return mve;
            } else if (plan instanceof Filter filter) {
                // gather all the filters' references to be checked later when a mv_expand is found
                filterReferences.addAll(filter.references());
            } else if (plan instanceof OrderBy) {
                // ordering after mv_expand COULD break the order of the results, so the limit shouldn't be copied past mv_expand
                // something like from test | sort emp_no | mv_expand job_positions | sort first_name | limit 5
                // (the sort first_name likely changes the order of the docs after sort emp_no, so "limit 5" shouldn't be copied down
                return null;
            }

            if (plan.child() instanceof UnaryPlan unaryPlan) {
                plan = unaryPlan;
            } else {
                break;
            }
        }
        return null;
    }

    private static Limit limitBeforeMvExpand(MvExpand mvExpand) {
        UnaryPlan plan = mvExpand;
        while (plan instanceof Aggregate == false) {
            if (plan instanceof Limit limit) {
                return limit;
            }
            if (plan.child() instanceof UnaryPlan unaryPlan) {
                plan = unaryPlan;
            } else {
                break;
            }
        }
        return null;
    }

    private LogicalPlan propagateDuplicateLimitUntilMvExpand(Limit duplicateLimit, MvExpand mvExpand, UnaryPlan child) {
        if (child == mvExpand) {
            return mvExpand.replaceChild(duplicateLimit);
        } else {
            return child.replaceChild(propagateDuplicateLimitUntilMvExpand(duplicateLimit, mvExpand, (UnaryPlan) child.child()));
        }
    }
}
