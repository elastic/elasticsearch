/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

class CategorizationBytesRefHash implements Releasable {

    /**
     * Our special wild card value.
     */
    static final BytesRef WILD_CARD_REF = new BytesRef("*");
    /**
     * For all WILD_CARD references, the token ID is always -1
     */
    static final int WILD_CARD_ID = -1;
    private final BytesRefHash bytesRefHash;

    CategorizationBytesRefHash(BytesRefHash bytesRefHash) {
        this.bytesRefHash = bytesRefHash;
    }

    int[] getIds(BytesRef[] tokens) {
        int[] ids = new int[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            ids[i] = put(tokens[i]);
        }
        return ids;
    }

    BytesRef[] getDeeps(int[] ids) {
        BytesRef[] tokens = new BytesRef[ids.length];
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = getDeep(ids[i]);
        }
        return tokens;
    }

    BytesRef getDeep(long id) {
        if (id == WILD_CARD_ID) {
            return WILD_CARD_REF;
        }
        BytesRef shallow = bytesRefHash.get(id, new BytesRef());
        return BytesRef.deepCopyOf(shallow);
    }

    int put(BytesRef bytesRef) {
        if (WILD_CARD_REF.equals(bytesRef)) {
            return WILD_CARD_ID;
        }
        long hash = bytesRefHash.add(bytesRef);
        if (hash < 0) {
            return (int) (-1L - hash);
        } else {
            if (hash > Integer.MAX_VALUE) {
                throw new AggregationExecutionException(
                    LoggerMessageFormat.format(
                        "more than [{}] unique terms encountered. "
                            + "Consider restricting the documents queried or adding [{}] in the {} configuration",
                        Integer.MAX_VALUE,
                        CategorizeTextAggregationBuilder.CATEGORIZATION_FILTERS.getPreferredName(),
                        CategorizeTextAggregationBuilder.NAME
                    )
                );
            }
            return (int) hash;
        }
    }

    @Override
    public void close() {
        bytesRefHash.close();
    }
}
