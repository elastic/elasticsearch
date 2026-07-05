/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;

/**
 * Converts any Analyzer-specific {@link ResolvingProject} into an {@link org.elasticsearch.xpack.esql.plan.logical.Project} equivalent.
 */
public class ResolvedProjects extends AnalyzerRules.AnalyzerRule<ResolvingProject> {

    @Override
    protected LogicalPlan rule(ResolvingProject plan) {
        return plan.asProject();
    }

    @Override
    protected boolean skipResolved() {
        return false;
    }
}
