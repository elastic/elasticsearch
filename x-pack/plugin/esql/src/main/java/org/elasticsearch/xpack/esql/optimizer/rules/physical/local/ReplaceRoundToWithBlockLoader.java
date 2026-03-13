/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Fallback for {@link ReplaceRoundToWithQueryAndTags}: when the query-and-tags
 * optimization cannot be applied (e.g. too many rounding points, sorts pushed
 * down, time-series constraints), this rule pushes the {@code RoundTo} function
 * into block loading via a {@link FunctionEsField} with a
 * {@link BlockLoaderFunctionConfig.RoundToLongs} config.
 * <p>
 * The resulting {@link FieldAttribute} is picked up by
 * {@link InsertFieldExtraction}, which inserts a {@code FieldExtractExec} that
 * loads values using the fused block loader.
 */
public class ReplaceRoundToWithBlockLoader extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    EvalExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(EvalExec evalExec, LocalPhysicalOptimizerContext ctx) {
        List<Alias> fields = evalExec.fields();
        List<Alias> updatedFields = null;
        for (int i = 0; i < fields.size(); i++) {
            Alias alias = fields.get(i);
            if (alias.child() instanceof RoundTo roundTo) {
                FieldAttribute replacement = tryReplace(roundTo, ctx);
                if (replacement != null) {
                    if (updatedFields == null) {
                        updatedFields = new ArrayList<>(fields);
                    }
                    updatedFields.set(i, alias.replaceChild(replacement));
                }
            }
        }
        if (updatedFields == null) {
            return evalExec;
        }
        return new EvalExec(evalExec.source(), evalExec.child(), updatedFields);
    }

    private static FieldAttribute tryReplace(RoundTo roundTo, LocalPhysicalOptimizerContext ctx) {
        BlockLoaderFunctionConfig.RoundToLongs config = roundTo.blockLoaderConfig();
        if (config == null) {
            return null;
        }
        FieldAttribute field = (FieldAttribute) roundTo.field();
        MappedFieldType.FieldExtractPreference preference = ctx.configuration().pragmas().fieldExtractPreference();
        if (ctx.searchStats().supportsLoaderConfig(field.fieldName(), config, preference) == false) {
            return null;
        }
        FunctionEsField functionEsField = new FunctionEsField(field.field(), roundTo.dataType(), config);
        String name = rawTemporaryName(field.name(), config.function().toString(), String.valueOf(Arrays.hashCode(config.points())));
        return new FieldAttribute(
            roundTo.source(),
            field.parentName(),
            field.qualifier(),
            name,
            functionEsField,
            field.nullable(),
            new NameId(),
            true
        );
    }
}
