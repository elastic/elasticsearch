/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.session.Result;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 *  could avoid that if we used a different structure instead of JSON, e.g., one column for names and one column for values.
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
                fieldNames.addAll(parseJson(getBytesRef(unmappedBlock, row, scratch)).keySet());
            }
        }
        return fieldNames;
    }

    /** Builds the expanded schema: every column except {@code _unmapped_fields}, then one keyword column per field name. */
    private static List<Attribute> buildSchema(List<Attribute> schema, int unmappedIdx, List<String> fieldNames) {
        List<Attribute> newSchema = new ArrayList<>(schema.size() - 1 + fieldNames.size());
        Set<String> existingNames = new HashSet<>();
        for (int i = 0; i < schema.size(); i++) {
            if (i != unmappedIdx) {
                Attribute attribute = schema.get(i);
                newSchema.add(attribute);
                existingNames.add(attribute.name());
            }
        }
        for (String name : fieldNames) {
            if (existingNames.contains(name)) {
                throw new IllegalStateException(
                    Strings.format(
                        "Conflict in unmapped field name: field '%s' appears both in the query schema and in the _unmapped_fields JSON",
                        name
                    )
                );
            }
            newSchema.add(new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD));
        }
        return newSchema;
    }

    /** Rewrite each page, replacing the {@code _unmapped_fields} block with one block per expanded field name. */
    private static List<Page> rewritePages(Result result, int unmappedIdx, int blockCount, List<String> fieldNames, BlockFactory factory) {
        int originalColumnCount = result.schema().size();
        BytesRef scratch = new BytesRef();
        var newPages = new ArrayList<Page>(result.pages().size());
        var success = false;
        try {
            for (Page p : result.pages()) {
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
        List<String> fieldNames,
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
                var rowMap = unmappedBlock.isNull(row) ? Map.of() : parseJson(getBytesRef(unmappedBlock, row, scratch));
                int builderIndex = 0;
                for (String fieldName : fieldNames) {
                    appendJsonValue(builders[builderIndex++], rowMap.get(fieldName));
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
            Releasables.closeExpectNoException(builders);
            if (success == false) {
                Releasables.closeExpectNoException(allBlocks);
            }
        }
    }

    /**
     * Emits a parsed JSON value to {@code builder}. Scalars become a single keyword; JSON arrays become a multi-value
     * position with one keyword per element (nested arrays flattened, null elements skipped, embedded objects as
     * canonical JSON text); top-level JSON objects become canonical JSON text in a single keyword position.
     */
    private static void appendJsonValue(BytesRefBlock.Builder builder, Object value) {
        if (value == null) {
            builder.appendNull();
            return;
        }
        if (value instanceof Map<?, ?> map) {
            builder.appendBytesRef(canonicalJsonBytesRef(map));
            return;
        }
        if (value instanceof List<?> list) {
            appendArrayAsMultiValue(builder, list);
            return;
        }
        builder.appendBytesRef(new BytesRef(String.valueOf(value)));
    }

    private static void appendArrayAsMultiValue(BytesRefBlock.Builder builder, List<?> list) {
        List<BytesRef> elements = new ArrayList<>();
        collectKeywordValues(list, elements);
        if (elements.isEmpty()) {
            builder.appendNull();
            return;
        }
        if (elements.size() == 1) {
            builder.appendBytesRef(elements.get(0));
            return;
        }
        builder.beginPositionEntry();
        for (BytesRef element : elements) {
            builder.appendBytesRef(element);
        }
        builder.endPositionEntry();
    }

    /**
     * Appends every array element in document order. Recurses into nested arrays (flattening scalar leaves),
     * serializes embedded objects to canonical JSON keyword elements, and skips JSON nulls.
     */
    private static void collectKeywordValues(List<?> list, List<BytesRef> elements) {
        for (Object element : list) {
            if (element == null) {
                continue;
            }
            if (element instanceof List<?> nested) {
                collectKeywordValues(nested, elements);
            } else if (element instanceof Map<?, ?> map) {
                elements.add(canonicalJsonBytesRef(map));
            } else {
                elements.add(new BytesRef(String.valueOf(element)));
            }
        }
    }

    private static BytesRef canonicalJsonBytesRef(Object value) {
        try (XContentBuilder json = XContentFactory.jsonBuilder()) {
            json.value(value);
            return BytesReference.bytes(json).toBytesRef();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static BytesRef getBytesRef(BytesRefBlock unmappedBlock, int row, BytesRef scratch) {
        if (unmappedBlock.getValueCount(row) != 1) {
            throw new IllegalStateException(
                Strings.format(
                    "Expected exactly one value in _unmapped_fields block at row %d, but got %d",
                    row,
                    unmappedBlock.getValueCount(row)
                )
            );
        }
        return unmappedBlock.getBytesRef(unmappedBlock.getFirstValueIndex(row), scratch);
    }

    private static Map<String, Object> parseJson(BytesRef ref) {
        return XContentHelper.convertToMap(new BytesArray(ref.bytes, ref.offset, ref.length), false, XContentType.JSON).v2();
    }

    private ExpandUnmappedFieldsPostProcessor() {/* static class. */}
}
