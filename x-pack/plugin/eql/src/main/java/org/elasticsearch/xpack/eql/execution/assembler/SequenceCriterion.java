/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.search.Timestamp;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.util.List;

public class SequenceCriterion extends Criterion<BoxedQueryRequest> {

    private final int stage;
    private final BoxedQueryRequest queryRequest;
    private final List<HitExtractor> keys;
    private final HitExtractor timestamp;
    private final HitExtractor tiebreaker;
    private final HitExtractor implicitTiebreaker;

    private final boolean descending;
    private final boolean missing;

    public SequenceCriterion(
        int stage,
        BoxedQueryRequest queryRequest,
        List<HitExtractor> keys,
        HitExtractor timestamp,
        HitExtractor tiebreaker,
        HitExtractor implicitTiebreaker,
        boolean descending,
        boolean missing
    ) {
        super(keys.size());
        this.stage = stage;
        this.queryRequest = queryRequest;
        this.keys = keys;
        this.timestamp = timestamp;
        this.tiebreaker = tiebreaker;
        this.implicitTiebreaker = implicitTiebreaker;
        this.descending = descending;
        this.missing = missing;
    }

    public int stage() {
        return stage;
    }

    public boolean descending() {
        return descending;
    }

    public BoxedQueryRequest queryRequest() {
        return queryRequest;
    }

    public Object[] key(SearchHit hit) {
        Object[] key = null;
        if (keySize() > 0) {
            Object[] docKeys = new Object[keySize()];
            for (int i = 0; i < keySize(); i++) {
                docKeys[i] = keys.get(i).extract(hit);
            }
            key = docKeys;
        }
        return key;
    }

    @SuppressWarnings({ "unchecked" })
    public Ordinal ordinal(SearchHit hit) {
        Timestamp ts = timestamp(hit);

        Comparable<Object> tbreaker = null;
        if (tiebreaker != null) {
            Object tb = tiebreaker.extract(hit);
            if (tb != null && tb instanceof Comparable == false) {
                // values come straight from the fields API, so a non-comparable one (a multi-valued field's list) is the data's fault
                throw new InvalidArgumentException(
                    "Expected tiebreaker field [{}] to hold a single comparable value but got [{}]",
                    fieldName(tiebreaker),
                    tb
                );
            }
            tbreaker = (Comparable<Object>) tb;
        }

        Object implicitTbreaker = implicitTiebreaker.extract(hit);
        if (implicitTbreaker instanceof Number == false) {
            throw new EqlIllegalArgumentException("Expected _shard_doc/implicit tiebreaker as long but got [{}]", implicitTbreaker);
        }
        long timebreakerValue = ((Number) implicitTbreaker).longValue();
        return new Ordinal(ts, tbreaker, timebreakerValue);
    }

    public boolean missing() {
        return missing;
    }

    public Timestamp timestamp(SearchHit hit) {
        Object ts = timestamp.extract(hit);
        if (ts instanceof Timestamp t) {
            return t;
        }
        // Only a single date or date_nanos value extracts as a Timestamp, so anything else stems from the data or its mapping
        if (ts == null) {
            throw new InvalidArgumentException(
                "Expected timestamp field [{}] to have a value but got none; check that [_source] is enabled and includes the field",
                fieldName(timestamp)
            );
        }
        throw new InvalidArgumentException(
            "Expected timestamp field [{}] to be mapped as [date] or [date_nanos] and to hold a single value but got [{}]",
            fieldName(timestamp),
            ts
        );
    }

    private static String fieldName(HitExtractor extractor) {
        return extractor instanceof AbstractFieldHitExtractor fieldExtractor ? fieldExtractor.fieldName() : String.valueOf(extractor);
    }

    @Override
    public String toString() {
        return "[" + stage + "][" + descending + "]";
    }
}
