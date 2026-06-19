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
 * Coordinator-side post-processor for {@code SET unmapped_fields="LOAD_ALL_EXPAND"}.
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

        // Pass 1 — collect all unique field names (sorted) across all pages.
        TreeSet<String> fieldNamesSet = new TreeSet<>();
        BytesRef scratch = new BytesRef();
        for (Page page : result.pages()) {
            BytesRefBlock unmappedBlock = (BytesRefBlock) page.getBlock(unmappedIdx);
            for (int row = 0; row < unmappedBlock.getPositionCount(); row++) {
                if (unmappedBlock.isNull(row)) {
                    continue;
                }
                BytesRef jsonRef = unmappedBlock.getBytesRef(unmappedBlock.getFirstValueIndex(row), scratch);
                Map<String, Object> parsed = parseJson(jsonRef);
                fieldNamesSet.addAll(parsed.keySet());
            }
        }
        List<String> sortedFieldNames = new ArrayList<>(fieldNamesSet);

        // Build the new schema: all columns except _unmapped_fields, then one column per field name.
        List<Attribute> newSchema = new ArrayList<>(schema.size() - 1 + sortedFieldNames.size());
        for (int i = 0; i < schema.size(); i++) {
            if (i != unmappedIdx) {
                newSchema.add(schema.get(i));
            }
        }
        for (String name : sortedFieldNames) {
            newSchema.add(new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD));
        }

        // Pass 2 — rewrite pages.
        List<Page> newPages = new ArrayList<>(result.pages().size());
        for (Page page : result.pages()) {
            int rowCount = page.getPositionCount();
            BytesRefBlock unmappedBlock = (BytesRefBlock) page.getBlock(unmappedIdx);

            // Parse all rows once.
            List<Map<String, Object>> rowMaps = new ArrayList<>(rowCount);
            for (int row = 0; row < rowCount; row++) {
                if (unmappedBlock.isNull(row)) {
                    rowMaps.add(Map.of());
                } else {
                    BytesRef jsonRef = unmappedBlock.getBytesRef(unmappedBlock.getFirstValueIndex(row), scratch);
                    rowMaps.add(parseJson(jsonRef));
                }
            }

            // Collect blocks from original page, skipping the _unmapped_fields block.
            Block[] allBlocks = new Block[newSchema.size()];
            int dest = 0;
            for (int i = 0; i < schema.size(); i++) {
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
                            String str = val instanceof String s ? s : String.valueOf(val);
                            builder.appendBytesRef(new BytesRef(str));
                        }
                    }
                    allBlocks[dest++] = builder.build();
                }
            }

            newPages.add(new Page(allBlocks));
        }

        return new Result(
            newSchema,
            newPages,
            result.attributeMetadata(),
            result.configuration(),
            result.completionInfo(),
            result.executionInfo(),
            false
        );
    }

    private static Map<String, Object> parseJson(BytesRef ref) {
        BytesArray bytes = new BytesArray(ref.bytes, ref.offset, ref.length);
        return XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
    }
}
