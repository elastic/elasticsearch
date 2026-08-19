/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles {@code METADATA} output for views.
 */
public final class ViewMetadataFieldRewriter {

    public static final String ID_SEPARATOR = "/";

    private ViewMetadataFieldRewriter() {}

    /**
     * Returns the view body wrapped with the rewrites required by the calling query's metadata
     * request, or the body unchanged when no requested metadata field needs special handling.
     *
     * @param viewName the name the view
     * @param viewBody the parsed view body
     * @param outerMetadataFields the metadata fields requested by the referencing {@code FROM}
     */
    public static LogicalPlan rewrite(String viewName, LogicalPlan viewBody, List<NamedExpression> outerMetadataFields) {
        Source source = viewBody.source();
        List<Alias> aliases = new ArrayList<>();
        boolean needsId = false;

        for (NamedExpression metadataField : outerMetadataFields) {
            switch (metadataField.name()) {
                case MetadataAttribute.INDEX -> aliases.add(
                    new Alias(source, MetadataAttribute.INDEX, Literal.keyword(source, viewName))
                );
                case IdFieldMapper.NAME -> {
                    needsId = true;
                    aliases.add(
                        new Alias(
                            source,
                            IdFieldMapper.NAME,
                            new Concat(
                                source,
                                Literal.keyword(source, viewName + ID_SEPARATOR),
                                List.of(new UnresolvedAttribute(source, IdFieldMapper.NAME))
                            )
                        )
                    );
                }
            }
        }

        if (aliases.isEmpty()) {
            return viewBody;
        }
        if (needsId) {
            viewBody = injectIdMetadata(viewBody, source);
        }
        return new Eval(source, viewBody, aliases);
    }

    /**
     * Traverses the view body and adds {@code _id} to the metadata fields of every
     * {@link UnresolvedRelation} leaf that does not already request it; without this, the
     * {@code CONCAT} rewrite injected by {@link #rewrite} would fail with a missing column.
     */
    private static LogicalPlan injectIdMetadata(LogicalPlan viewBody, Source source) {
        MetadataAttribute idAttr = (MetadataAttribute) MetadataAttribute.create(source, IdFieldMapper.NAME);
        return viewBody.transformDown(UnresolvedRelation.class, ur -> {
            boolean hasId = ur.metadataFields().stream().anyMatch(f -> IdFieldMapper.NAME.equals(f.name()));
            return hasId ? ur : ur.addMetadataField(idAttr);
        });
    }
}
