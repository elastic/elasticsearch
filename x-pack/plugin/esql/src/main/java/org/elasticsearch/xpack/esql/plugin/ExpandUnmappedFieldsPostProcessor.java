/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Coordinator-side post-processor for {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>After all data-node pages are collected, scans the {@code _unmapped_fields} column
 * (a JSON-object keyword column) to discover the full set of unique field names across
 * all rows, then replaces that column with one dedicated {@code keyword} column per
 * unique field name. Rows that do not carry a given field get {@code null} in that column.
 * <p>
 * TODO we double parse the JSON here: once for computing the union of all the field names, and the second for the actual expansion. We
 *  could avoid that if we used a different structure instead of JSON here, e.g., one column for names and one column for values.
 */
class ExpandUnmappedFieldsPostProcessor {
    /**
     * Expands the {@code _unmapped_fields} column in {@code result} into per-field columns.
     * Returns {@code result} unchanged if no {@link UnmappedFieldsAttribute} is present in the schema.
     */
    static Result expand(Result result, BlockFactory blockFactory) {
        List<Attribute> schema = result.schema();

        int unmappedIdx = CollectionUtils.findIndex(schema, e -> e instanceof UnmappedFieldsAttribute);
        if (unmappedIdx == -1) {
            return result;
        }

        List<String> sortedFieldNames = new ArrayList<>(collectFieldNames(result, unmappedIdx));
        List<Attribute> newSchema = buildSchema(schema, unmappedIdx, sortedFieldNames);
        List<Page> newPages = rewritePages(result, unmappedIdx, newSchema.size(), sortedFieldNames, blockFactory);

        return new Result(
            newSchema,
            newPages,
            result.attributeMetadata(),
            result.configuration(),
            result.completionInfo(),
            result.executionInfo()
        );
    }

    /** Collect the unique field names (sorted) carried by {@code _unmapped_fields} across all pages. */
    private static SortedSet<String> collectFieldNames(Result result, int unmappedIdx) {
        TreeSet<String> fieldNames = new TreeSet<>();
        BytesRef scratch = new BytesRef();
        for (Page page : result.pages()) {
            BytesRefBlock unmappedBlock = page.getBlock(unmappedIdx);
            for (int row = 0; row < unmappedBlock.getPositionCount(); row++) {
                if (unmappedBlock.isNull(row)) {
                    continue;
                }
                fieldNames.addAll(parseJson(unmappedBlock.getBytesRef(unmappedBlock.getFirstValueIndex(row), scratch)).keySet());
            }
        }
        return fieldNames;
    }

    /** Builds the expanded schema: every column except {@code _unmapped_fields}, then one keyword column per field name. */
    private static List<Attribute> buildSchema(List<Attribute> schema, int unmappedIdx, List<String> sortedFieldNames) {
        List<Attribute> newSchema = new ArrayList<>(schema.size() - 1 + sortedFieldNames.size());
        for (int i = 0; i < schema.size(); i++) {
            if (i != unmappedIdx) {
                newSchema.add(schema.get(i));
            }
        }
        for (String name : sortedFieldNames) {
            newSchema.add(new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD));
        }
        return newSchema;
    }

    /** Rewrite each page, replacing the {@code _unmapped_fields} block with one block per expanded field name. */
    private static List<Page> rewritePages(Result res, int unmappedIdx, int blockCount, List<String> fieldNames, BlockFactory factory) {
        int originalColumnCount = res.schema().size();
        BytesRef scratch = new BytesRef();
        var newPages = new ArrayList<Page>(res.pages().size());
        var success = false;
        try {
            for (Page p : res.pages()) {
                newPages.add(rewritePage(unmappedIdx, blockCount, fieldNames, factory, p, scratch, originalColumnCount));
            }
            success = true;
            return newPages;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(newPages);
            }
        }
    }

    private static Page rewritePage(
        int unmappedIdx,
        int blockCount,
        List<String> sortedFieldNames,
        BlockFactory blockFactory,
        Page page,
        BytesRef scratch,
        int originalColumnCount
    ) {

        // Collect blocks from original page, skipping the _unmapped_fields block.
        Block[] allBlocks = new Block[blockCount];
        int dest = 0;
        for (int i = 0; i < originalColumnCount; i++) {
            if (i != unmappedIdx) {
                var block = page.getBlock(i);
                block.incRef();
                allBlocks[dest++] = block;
            }
        }

        var success = false;
        int retainedBlockCount = originalColumnCount - 1;
        BytesRefBlock.Builder[] builders = new BytesRefBlock.Builder[blockCount - retainedBlockCount];
        BytesRefBlock unmappedBlock = page.getBlock(unmappedIdx);
        try {
            Arrays.setAll(builders, i -> blockFactory.newBytesRefBlockBuilder(page.getPositionCount()));
            for (int row = 0; row < page.getPositionCount(); row++) {
                var rowMap = unmappedBlock.isNull(row)
                    ? Map.of()
                    : parseJson(unmappedBlock.getBytesRef(unmappedBlock.getFirstValueIndex(row), scratch));
                int builderIndex = 0;
                for (String fieldName : sortedFieldNames) {
                    var builder = builders[builderIndex++];
                    Object val = rowMap.get(fieldName);
                    if (val == null) {
                        builder.appendNull();
                    } else {
                        builder.appendBytesRef(new BytesRef(String.valueOf(val)));
                    }
                }
            }
            for (int i = 0; i < builders.length; i++) {
                allBlocks[retainedBlockCount + i] = builders[i].build();
            }
            var result = new Page(allBlocks);
            // Release _unmapped_fields block from the circuit breaker; the surviving blocks were protected by incRef above.
            page.releaseBlocks();
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(allBlocks);
            }
        }
    }

    private static Map<String, Object> parseJson(BytesRef ref) {
        BytesArray bytes = new BytesArray(ref.bytes, ref.offset, ref.length);
        return XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
    }

    private ExpandUnmappedFieldsPostProcessor() {/* static class. */}
}
