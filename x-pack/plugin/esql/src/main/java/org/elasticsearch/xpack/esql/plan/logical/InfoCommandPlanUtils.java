/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Shared helpers for {@link MetricsInfo} and {@link TsInfo} plan construction.
 * <p>
 * Info commands require {@code _doc} metadata on the source relation so logical optimizers retain it
 * when union-type resolution injects Eval+Project nodes below the command (see #145766).
 */
public final class InfoCommandPlanUtils {

    private InfoCommandPlanUtils() {}

    /**
     * Ensures the TS source relation beneath an info command requests {@code _doc} metadata.
     */
    public static LogicalPlan injectDocAttribute(Source source, LogicalPlan input) {
        return input.transformDown(r -> {
            if (r instanceof UnresolvedRelation unresolved) {
                for (NamedExpression field : unresolved.metadataFields()) {
                    if (field.name().equals(MetadataAttribute.DOC)) {
                        return r;
                    }
                }
                return unresolved.addMetadataField(new MetadataAttribute(source, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false));
            }
            return r;
        });
    }
}
