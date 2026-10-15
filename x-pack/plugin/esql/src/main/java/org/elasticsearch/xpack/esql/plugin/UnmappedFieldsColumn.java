/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.UnmappedFields;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * The column that returns the unmapped fields of each row under {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * A shard writes the unmapped fields of a document as one JSON object (see {@link UnmappedFields#allFieldsBlockLoader}), and
 * the column returns that object as the shard wrote it. It holds the unmapped fields that would reach the output of the query
 * if they were mapped: the shard reads only those that the {@link UnmappedFieldsPattern} of the column includes.
 *
 * The column is planned as an {@link UnmappedFieldsAttribute}, whose name a query cannot refer to, and is returned as
 * {@value #NAME} with the type {@link DataType#SOURCE}, in the same position. A query that itself outputs a column named
 * {@value #NAME} keeps that column, and the unmapped fields are not returned.
 */
public final class UnmappedFieldsColumn {
    /** The name the column is returned under. */
    public static final String NAME = "_unmapped_fields";

    /**
     * Returns {@code result} with its {@link UnmappedFieldsAttribute} column, if it has one, named {@value #NAME}. The pages
     * of {@code result} are released if this fails.
     */
    public static Result expose(Result result) {
        List<Attribute> schema = result.schema();
        int unmappedIdx = CollectionUtils.findIndex(schema, e -> e instanceof UnmappedFieldsAttribute);
        if (unmappedIdx == -1) {
            return result;
        }
        for (int c = 0; c < schema.size(); c++) {
            if (c != unmappedIdx && schema.get(c).name().equals(NAME)) {
                return withoutColumn(result, unmappedIdx);
            }
        }
        List<Attribute> newSchema = new ArrayList<>(schema);
        newSchema.set(unmappedIdx, new ReferenceAttribute(Source.EMPTY, NAME, DataType.SOURCE));
        return withSchemaAndPages(result, newSchema, result.pages());
    }

    private static Result withoutColumn(Result result, int unmappedIdx) {
        List<Attribute> schema = result.schema();
        int[] keep = new int[schema.size() - 1];
        List<Attribute> newSchema = new ArrayList<>(keep.length);
        for (int c = 0, k = 0; c < schema.size(); c++) {
            if (c != unmappedIdx) {
                keep[k++] = c;
                newSchema.add(schema.get(c));
            }
        }
        List<Page> newPages = new ArrayList<>(result.pages().size());
        boolean success = false;
        try {
            for (Page page : result.pages()) {
                newPages.add(page.projectBlocks(keep));
                page.releaseBlocks();
            }
            success = true;
            return withSchemaAndPages(result, newSchema, newPages);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(newPages);
                Releasables.closeExpectNoException(result.pages());
            }
        }
    }

    private static Result withSchemaAndPages(Result result, List<Attribute> schema, List<Page> pages) {
        return new Result(
            schema,
            pages,
            result.attributeMetadata(),
            result.configuration(),
            result.completionInfo(),
            result.executionInfo(),
            result.approximationApplied()
        );
    }

    private UnmappedFieldsColumn() {}
}
