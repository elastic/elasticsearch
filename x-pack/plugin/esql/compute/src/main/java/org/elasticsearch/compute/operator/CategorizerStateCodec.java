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
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Wire-format codec shared by {@link CategorizeEvalOperator} (write) and
 * {@link CategorizeGroupingMergeOperator} (read + write in INTERMEDIATE mode).
 *
 * <p>Format: {@code boolean seenNull} followed by {@code int count} (varint) followed by
 * {@code count} serialized {@link SerializableTokenListCategory} values.
 *
 * <p>This format is byte-identical with {@code CategorizeBlockHash.serializeCategorizer()}.
 */
public class CategorizerStateCodec {

    public static final int NULL_ORD = 0;

    public CategorizerStateCodec() {}

    /**
     * Store whether we've seen any {@code null} values.
     * <p>
     *     Null gets the {@link #NULL_ORD} ord.
     * </p>
     */
    private boolean seenNull = false;

    public boolean getSeenNull() {
        return seenNull;
    }

    public void setSeenNull() {
        this.seenNull = true;
    }

    /**
     * Serializes the current categorizer state to a {@link BytesRef}.
     *
     * @param seenNull {@code true} if any null or zero-token value was encountered; maps to
     *                 {@link #NULL_ORD} in the row data and must be propagated so downstream
     *                 operators can correctly reconstruct the null group when needed.
     */
    public static BytesRef serialize(TokenListCategorizer.CloseableTokenListCategorizer categorizer, boolean seenNull) {
        // TODO: This BytesStreamOutput is not accounted for by the circuit breaker. Fix that!
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeBoolean(seenNull);
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
     * Gets the state already deserialized {@link BytesRef} and merges each category
     * into {@code globalCategorizer} via
     * {@link TokenListCategorizer#mergeWireCategory}, building and returning a
     * local-ordinal → global-ordinal map.
     *
     * <p>{@link #NULL_ORD} maps to itself only when the serialized state has {@code seenNull=true}.
     * Callers that use {@link Map#getOrDefault} with {@link #NULL_ORD} as the default are
     * unaffected by its absence.
     */
    public Map<Integer, Integer> buildIdMap(BytesRef stateBytes, TokenListCategorizer.CloseableTokenListCategorizer globalCategorizer) {
        Map<Integer, Integer> idMap = new HashMap<>();

        try (StreamInput in = new BytesArray(stateBytes).streamInput()) {
            if (in.readBoolean()) {  // seenNull
                this.seenNull = true;
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
