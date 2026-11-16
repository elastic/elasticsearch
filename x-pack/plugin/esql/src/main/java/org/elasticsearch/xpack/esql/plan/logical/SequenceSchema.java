/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;

/** Carries column restrictions and declared analysis across materialization without claiming an indexed source field. */
final class SequenceSchema {
    private SequenceSchema() {}

    /** The same typed boundary applies to wide outputs and to protected inputs of an earlier-member condition. */
    static Attribute reference(Attribute source, Source location, String name, @Nullable NameId id, boolean synthetic) {
        Attribute typed = source;
        if (source instanceof FieldAttribute field
            && field.field() instanceof TypeConflictedField conflict
            && conflict.isSingleTypePotentiallyUnmapped() == false) {
            typed = field.flagTypeConflicts();
        }
        if (typed instanceof UnsupportedAttribute unsupported) {
            return new UnsupportedAttribute(location, null, name, unsupported.field(), unsupported.unresolvedMessage(), id);
        }
        String valuesAnalyzer = AnalyzedTextExpression.valuesAnalyzerOf(source);
        if (valuesAnalyzer == null && source instanceof FieldAttribute field && field.field() instanceof TextEsField text) {
            // Field caps reports a name only when it can be resolved outside the index's own analysis settings.
            valuesAnalyzer = text.analyzerName();
        }
        return new ReferenceAttribute(location, null, name, source.dataType(), source.nullable(), id, synthetic, valuesAnalyzer);
    }

    /** Compiler labels are pass-through projections; ordinary user aliases still receive the ordinary unsupported-field error. */
    static NamedExpression projection(Attribute value, String name, @Nullable NameId id, boolean synthetic) {
        return value instanceof UnsupportedAttribute
            ? reference(value, value.source(), name, id == null ? value.id() : id, synthetic)
            : new Alias(value.source(), name, value, id, synthetic);
    }
}
