/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.StringUtils.WILDCARD;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;

public class FromClausesFields {
    public final List<Attribute> metadata;
    public final List<NamedExpression> optional;
    public final List<NamedExpression> unmapped;

    record FieldsClauseContexts(
        EsqlBaseParser.MetadataFieldsContext metadata,
        EsqlBaseParser.OptionalFieldsContext optional,
        EsqlBaseParser.UnmappedFieldsContext unmapped
    ) {
        static FieldsClauseContexts from(EsqlBaseParser.IndexPatternAndFieldsContext ctx) {
            EsqlBaseParser.MetadataFieldsContext metadataCtx = null;
            EsqlBaseParser.OptionalFieldsContext optionalCtx = null;
            EsqlBaseParser.UnmappedFieldsContext unmappedCtx = null;

            for (var clause : ctx.fieldsClause()) {
                if (clause.metadataFields() != null) {
                    checkSingleInvocation("METADATA", clause, metadataCtx);
                    metadataCtx = clause.metadataFields();
                } else if (clause.optionalFields() != null) {
                    checkSingleInvocation("OPTIONAL", clause, optionalCtx);
                    optionalCtx = clause.optionalFields();
                } else if (clause.unmappedFields() != null) {
                    checkSingleInvocation("UNMAPPED", clause, unmappedCtx);
                    unmappedCtx = clause.unmappedFields();
                }
            }

            return new FieldsClauseContexts(metadataCtx, optionalCtx, unmappedCtx);
        }

        private static void checkSingleInvocation(
            String clause,
            EsqlBaseParser.FieldsClauseContext repeatedOccurrence,
            ParserRuleContext firstOccurrence
        ) {
            if (firstOccurrence != null) {
                throw new ParsingException(
                    source(repeatedOccurrence),
                    "{} clause specified more than once; the first was: [{}]",
                    clause,
                    source(firstOccurrence).text()
                );
            }
        }
    }

    private static List<Attribute> metadataFields(EsqlBaseParser.MetadataFieldsContext ctx) {
        if (ctx == null) {
            return List.of();
        }
        Map<String, Attribute> metadataMap = new LinkedHashMap<>();
        for (var c : ctx.UNQUOTED_SOURCE()) {
            String id = c.getText();
            Source src = source(c);
            if (MetadataAttribute.isSupported(id) == false) {
                throw new ParsingException(src, "unsupported metadata field [{}]", id);
            }
            Attribute a = metadataMap.put(id, MetadataAttribute.create(src, id));
            if (a != null) {
                throw new ParsingException(src, "metadata field [{}] already declared [{}]", id, a.source().source());
            }
        }
        return List.of(metadataMap.values().toArray(Attribute[]::new));
    }

    private List<NamedExpression> fieldNamePatterns(
        Function<EsqlBaseParser.FieldNamePatternsContext, List<NamedExpression>> patternsVisitor,
        Supplier<EsqlBaseParser.FieldNamePatternsContext> ctxSupplier,
        String clause
    ) {
        if (ctxSupplier == null) {
            return List.of();
        }
        var ctx = ctxSupplier.get();
        var names = patternsVisitor.apply(ctx);
        LinkedHashMap<String, NamedExpression> namesMap = new LinkedHashMap<>(names.size());
        boolean hasStar = false;
        for (var na : names) {
            hasStar = hasStar || na instanceof UnresolvedStar;
            if (hasStar) {
                if (namesMap.size() > 0) {
                    throw new ParsingException(
                        na.source(),
                        "wildcard ({}) must be present only once and be the only {} field when present, found [{}]",
                        WILDCARD,
                        clause,
                        ctx.getText()
                    );
                }
                namesMap.putLast(WILDCARD, na);
            } else {
                namesMap.putLast(na.name(), na);
            }
        }
        return List.copyOf(namesMap.values());
    }

    public FromClausesFields(LogicalPlanBuilder planBuilder, EsqlBaseParser.IndexPatternAndFieldsContext fieldsCtx) {
        var clauseCtxs = FieldsClauseContexts.from(fieldsCtx);
        this.metadata = metadataFields(clauseCtxs.metadata);
        this.optional = fieldNamePatterns(
            planBuilder::visitFieldNamePatterns,
            () -> clauseCtxs.optional == null ? null : clauseCtxs.optional.fieldNamePatterns(),
            "optional"
        );
        this.unmapped = fieldNamePatterns(
            planBuilder::visitFieldNamePatterns,
            () -> clauseCtxs.unmapped == null ? null : clauseCtxs.unmapped.fieldNamePatterns(),
            "unmapped"
        );
    }
}
