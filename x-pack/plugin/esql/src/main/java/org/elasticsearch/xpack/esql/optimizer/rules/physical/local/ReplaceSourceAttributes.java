/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.UP;

public class ReplaceSourceAttributes extends PhysicalOptimizerRules.OptimizerRule<EsSourceExec> {

    public ReplaceSourceAttributes() {
        super(UP);
    }

    @Override
    protected PhysicalPlan rule(EsSourceExec plan) {
        final List<Attribute> attributes = new ArrayList<>();
        attributes.add(getDocAttribute(plan));

        if (plan.indexMode() == IndexMode.TIME_SERIES) {
            for (EsField field : EsQueryExec.TIME_SERIES_SOURCE_FIELDS) {
                attributes.add(new FieldAttribute(plan.source(), null, null, field.getName(), field));
            }
        } else {
            for (Attribute attr : plan.output()) {
                if (attr instanceof MetadataAttribute ma && ma.name().equals(MetadataAttribute.SCORE)) {
                    attributes.add(attr);
                    break;
                }
            }
        }

        return new EsQueryExec(
            plan.source(),
            plan.indexPattern(),
            plan.indexMode(),
            attributes,
            null,
            null,
            null,
            List.of(new EsQueryExec.QueryBuilderAndTags(plan.query(), List.of()))
        );
    }

    private static Attribute getDocAttribute(EsSourceExec plan) {
        // The source (or doc) field is sometimes added to the relation output as a hack to enable late materialization in the reduce
        // driver. In that case, we should take it instead of replacing it with a new one to ensure the same attribute is used throughout.
        var sourceAttributes = plan.output().stream().filter(EsQueryExec::isDocAttribute).toList();
        if (sourceAttributes.size() > 1) {
            throw new IllegalStateException("Expected at most one source attribute, found: " + sourceAttributes);
        }
        if (sourceAttributes.isEmpty()) {
            return new FieldAttribute(plan.source(), null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        }
        return sourceAttributes.getFirst();
    }
}
