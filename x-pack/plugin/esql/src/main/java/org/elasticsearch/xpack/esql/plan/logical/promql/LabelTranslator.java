/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.LabelTranslationContext.IntermediateResult;

/**
 * Translates each PromQL node over a label container, {@link LabelTranslationContext}, next to the translation over label
 * columns that {@link PromqlPlan#translate} still does. A node joins once its translation over the label container exists.
 */
final class LabelTranslator {

    private LabelTranslator() {}

    static IntermediateResult translate(LogicalPlan node, LabelTranslationContext context) {
        switch (node) {
            default -> throw new QlIllegalArgumentException(
                "PromQL plan node [{}] does not translate over a label container yet",
                node.nodeName()
            );
        }
    }
}
