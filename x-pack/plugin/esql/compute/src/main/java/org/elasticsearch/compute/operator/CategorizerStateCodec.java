/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Wire-format codec shared by {@link CategorizeGroupingOperator} (write) and
 * {@link CategorizeGroupingMergeOperator} (read + write in INTERMEDIATE mode).
 *
 * <p>Format: {@code boolean seenNull} (always {@code false}; the null ordinal is encoded as
 * {@link #NULL_ORD}) followed by {@code int count} (varint) followed by {@code count}
 * serialized {@link SerializableTokenListCategory} values.
 *
 * <p>This format must stay byte-identical with {@code CategorizeBlockHash.serializeCategorizer()}.
 */
final class CategorizerStateCodec {

    static final int NULL_ORD = 0;

    private CategorizerStateCodec() {}

    /**
     * Serializes the current categorizer state to a {@link BytesRef}.
     */
    static BytesRef serialize(TokenListCategorizer.CloseableTokenListCategorizer categorizer) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeBoolean(false); // seenNull: uses NULL_ORD=0, not a separate null flag
            int count = categorizer.getCategoryCount();
            out.writeVInt(count);
            for (SerializableTokenListCategory category : categorizer.toCategoriesById()) {
                category.writeTo(out);
            }
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserializes the state from a constant {@link BytesRefBlock} and merges each category
     * into {@code globalCategorizer} via
     * {@link TokenListCategorizer#mergeWireCategory}, building and returning a
     * local-ordinal → global-ordinal map.
     *
     * <p>{@link #NULL_ORD} always maps to itself.
     */
    static Map<Integer, Integer> buildIdMap(
        BytesRefBlock stateBlock,
        TokenListCategorizer.CloseableTokenListCategorizer globalCategorizer
    ) {
        Map<Integer, Integer> idMap = new HashMap<>();
        idMap.put(NULL_ORD, NULL_ORD);

        BytesRef stateBytes = stateBlock.getBytesRef(stateBlock.getFirstValueIndex(0), new BytesRef());
        try (StreamInput in = new BytesArray(stateBytes).streamInput()) {
            boolean seenNull = in.readBoolean();
            if (seenNull) {
                idMap.put(NULL_ORD, NULL_ORD);
            }
            int count = in.readVInt();
            for (int oldId = 0; oldId < count; oldId++) {
                int newGlobalId = globalCategorizer.mergeWireCategory(new SerializableTokenListCategory(in)).getId();
                idMap.put(oldId + 1, newGlobalId + 1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return idMap;
    }
}
