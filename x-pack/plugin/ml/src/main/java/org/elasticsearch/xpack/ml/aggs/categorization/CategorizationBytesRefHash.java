/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;

import java.io.Closeable;
import java.io.IOException;

class CategorizationBytesRefHash implements Closeable {

    static final BytesRef WILD_CARD_REF = new BytesRef("*");
    static final long WILD_CARD_ID = -1;
    private final BytesRefHash bytesRefHash;

    CategorizationBytesRefHash(BytesRefHash bytesRefHash) {
        this.bytesRefHash = bytesRefHash;
    }

    BytesRef getShallow(long id) {
        if (id == WILD_CARD_ID) {
            return WILD_CARD_REF;
        }
        return bytesRefHash.get(id, new BytesRef());
    }

    Long[] getIds(BytesRef[] tokens) {
        Long[] ids = new Long[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            ids[i] = put(tokens[i]);
        }
        return ids;
    }

    BytesRef[] getShallows(Long[] ids) {
        BytesRef[] tokens = new BytesRef[ids.length];
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = getShallow(ids[i]);
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

    long put(BytesRef bytesRef) {
        if (WILD_CARD_REF.equals(bytesRef)) {
            return WILD_CARD_ID;
        }
        long hash = bytesRefHash.add(bytesRef);
        if (hash < 0) {
            return -1 - hash;
        } else {
            return hash;
        }
    }

    @Override
    public void close() throws IOException {
        bytesRefHash.close();
    }
}
