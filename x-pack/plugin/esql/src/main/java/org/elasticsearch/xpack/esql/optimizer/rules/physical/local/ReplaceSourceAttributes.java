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
        var docId = new FieldAttribute(plan.source(), EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        final List<Attribute> attributes = new ArrayList<>();
        attributes.add(docId);
        if (plan.indexMode() == IndexMode.TIME_SERIES) {
            Attribute tsid = null, timestamp = null;
            for (Attribute attr : plan.output()) {
                String name = attr.name();
                if (name.equals(MetadataAttribute.TSID_FIELD)) {
                    tsid = attr;
                } else if (name.equals(MetadataAttribute.TIMESTAMP_FIELD)) {
                    timestamp = attr;
                }
            }
            if (tsid == null || timestamp == null) {
                throw new IllegalStateException("_tsid or @timestamp are missing from the time-series source");
            }
            attributes.add(tsid);
            attributes.add(timestamp);
        }
        plan.output().forEach(attr -> {
            if (attr instanceof MetadataAttribute ma && ma.name().equals(MetadataAttribute.SCORE)) {
                attributes.add(ma);
            }
        });
        return new EsQueryExec(plan.source(), plan.index(), plan.indexMode(), attributes, plan.query());
    }
}
