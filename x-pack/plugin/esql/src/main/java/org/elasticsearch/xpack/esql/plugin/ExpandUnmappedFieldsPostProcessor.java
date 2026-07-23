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
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Coordinator-side post-processor for {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>After all data-node pages are collected, scans the {@code _unmapped_fields} column
 * (a JSON-object keyword column) to discover the full set of unique field names across
 * all rows, then replaces that column with one dedicated {@code keyword} column per
 * unique field name. Rows that do not carry a given field get {@code null} in that column.
 */
class ExpandUnmappedFieldsPostProcessor {

    private ExpandUnmappedFieldsPostProcessor() {}

    /**
     * Expands the {@code _unmapped_fields} column in {@code result} into per-field columns.
     * Returns {@code result} unchanged if no {@link UnmappedFieldsAttribute} is present in the schema.
     */
    static Result expand(Result result, BlockFactory blockFactory) {
        List<Attribute> schema = result.schema();

        int unmappedIdx = -1;
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i) instanceof UnmappedFieldsAttribute) {
                unmappedIdx = i;
                break;
            }
        }
        if (unmappedIdx == -1) {
            return result;
        }

        List<String> sortedFieldNames = collectFieldNames(result, unmappedIdx);
        List<Attribute> newSchema = buildSchema(schema, unmappedIdx, sortedFieldNames);
        List<Page> newPages = rewritePages(result, unmappedIdx, newSchema, sortedFieldNames, blockFactory);

        return new Result(
            newSchema,
            newPages,
            result.attributeMetadata(),
            result.configuration(),
            result.completionInfo(),
            result.executionInfo()
        );
    }

    /** Pass 1 — collect the unique field names (sorted) carried by {@code _unmapped_fields} across all pages. */
    private static List<String> collectFieldNames(Result result, int unmappedIdx) {
        TreeSet<String> fieldNames = new TreeSet<>();
        BytesRef scratch = new BytesRef();
        for (Page page : result.pages()) {
            BytesRefBlock unmappedBlock = (BytesRefBlock) page.getBlock(unmappedIdx);
            for (int row = 0; row < unmappedBlock.getPositionCount(); row++) {
                if (unmappedBlock.isNull(row)) {
                    continue;
                }
                fieldNames.addAll(parseJson(unmappedBlock.getBytesRef(unmappedBlock.getFirstValueIndex(row), scratch)).keySet());
            }
        }
        return new ArrayList<>(fieldNames);
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

    /** Pass 2 — rewrite each page, replacing the {@code _unmapped_fields} block with one block per expanded field name. */
    private static List<Page> rewritePages(
        Result result,
        int unmappedIdx,
        List<Attribute> newSchema,
        List<String> sortedFieldNames,
        BlockFactory blockFactory
    ) {
        int columnCount = result.schema().size();
        List<Page> newPages = new ArrayList<>(result.pages().size());
        BytesRef scratch = new BytesRef();
        for (Page page : result.pages()) {
            int rowCount = page.getPositionCount();
            BytesRefBlock unmappedBlock = (BytesRefBlock) page.getBlock(unmappedIdx);

            List<Map<String, Object>> rowMaps = new ArrayList<>(rowCount);
            for (int row = 0; row < rowCount; row++) {
                rowMaps.add(
                    unmappedBlock.isNull(row)
                        ? Map.of()
                        : parseJson(unmappedBlock.getBytesRef(unmappedBlock.getFirstValueIndex(row), scratch))
                );
            }

            // Collect blocks from original page, skipping the _unmapped_fields block.
            Block[] allBlocks = new Block[newSchema.size()];
            int dest = 0;
            for (int i = 0; i < columnCount; i++) {
                if (i != unmappedIdx) {
                    page.getBlock(i).incRef();
                    allBlocks[dest++] = page.getBlock(i);
                }
            }

            // Build one new block per expanded field name.
            for (String fieldName : sortedFieldNames) {
                try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                    for (int row = 0; row < rowCount; row++) {
                        Object val = rowMaps.get(row).get(fieldName);
                        if (val == null) {
                            builder.appendNull();
                        } else {
                            builder.appendBytesRef(new BytesRef(val instanceof String s ? s : String.valueOf(val)));
                        }
                    }
                    allBlocks[dest++] = builder.build();
                }
            }

            newPages.add(new Page(allBlocks));
            // Release the original page now that all its blocks have been transferred
            // (incRef'd) or superseded. This releases the _unmapped_fields block from
            // the circuit breaker; the surviving blocks were protected by incRef above.
            page.releaseBlocks();
        }
        return newPages;
    }

    private static Map<String, Object> parseJson(BytesRef ref) {
        BytesArray bytes = new BytesArray(ref.bytes, ref.offset, ref.length);
        return XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
    }
}
