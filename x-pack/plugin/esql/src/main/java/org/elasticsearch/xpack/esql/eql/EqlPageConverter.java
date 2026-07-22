/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;

import java.util.List;

/**
 * Converts a bounded {@link EqlSearchResponse} into a single {@link Page} under the fixed schema of the
 * {@code EQL} source command (see {@link EqlRelation}). Kept separate from the source operator so the
 * conversion is unit-testable against a hand-built response, with no client or driver involved.
 *
 * <p>Column order must match {@link EqlRelation#buildOutput}:
 * <ul>
 *   <li>event mode: {@code _index, _id, _source}</li>
 *   <li>sequence/sample mode (unnested to one row per event): {@code _seq, _position, by, _index, _id, _source}</li>
 * </ul>
 */
public final class EqlPageConverter {

    private EqlPageConverter() {}

    public static Page toPage(EqlSearchResponse response, EqlRelation.Mode mode, BlockFactory blockFactory) {
        return mode == EqlRelation.Mode.EVENT ? eventsToPage(response, blockFactory) : sequencesToPage(response, blockFactory);
    }

    private static Page eventsToPage(EqlSearchResponse response, BlockFactory blockFactory) {
        List<Event> events = response.hits().events();
        int rows = events == null ? 0 : events.size();

        BytesRefBlock.Builder index = null;
        BytesRefBlock.Builder id = null;
        BytesRefBlock.Builder source = null;
        boolean success = false;
        try {
            index = blockFactory.newBytesRefBlockBuilder(rows);
            id = blockFactory.newBytesRefBlockBuilder(rows);
            source = blockFactory.newBytesRefBlockBuilder(rows);
            for (int i = 0; i < rows; i++) {
                appendEvent(events.get(i), index, id, source);
            }
            Page page = new Page(index.build(), id.build(), source.build());
            success = true;
            return page;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(index, id, source);
            }
        }
    }

    private static Page sequencesToPage(EqlSearchResponse response, BlockFactory blockFactory) {
        List<Sequence> sequences = response.hits().sequences();
        if (sequences == null) {
            sequences = List.of();
        }
        int rows = 0;
        for (Sequence sequence : sequences) {
            rows += sequence.events().size();
        }

        LongBlock.Builder seq = null;
        IntBlock.Builder position = null;
        BytesRefBlock.Builder by = null;
        BytesRefBlock.Builder index = null;
        BytesRefBlock.Builder id = null;
        BytesRefBlock.Builder source = null;
        boolean success = false;
        try {
            seq = blockFactory.newLongBlockBuilder(rows);
            position = blockFactory.newIntBlockBuilder(rows);
            by = blockFactory.newBytesRefBlockBuilder(rows);
            index = blockFactory.newBytesRefBlockBuilder(rows);
            id = blockFactory.newBytesRefBlockBuilder(rows);
            source = blockFactory.newBytesRefBlockBuilder(rows);

            for (int s = 0; s < sequences.size(); s++) {
                Sequence sequence = sequences.get(s);
                List<Event> events = sequence.events();
                for (int p = 0; p < events.size(); p++) {
                    seq.appendLong(s);
                    position.appendInt(p);
                    appendJoinKeys(sequence.joinKeys(), by);
                    appendEvent(events.get(p), index, id, source);
                }
            }
            Page page = new Page(seq.build(), position.build(), by.build(), index.build(), id.build(), source.build());
            success = true;
            return page;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(seq, position, by, index, id, source);
            }
        }
    }

    /** Appends the {@code _index}, {@code _id}, {@code _source} triple for one event, or nulls for a missing event. */
    private static void appendEvent(Event event, BytesRefBlock.Builder index, BytesRefBlock.Builder id, BytesRefBlock.Builder source) {
        if (event == null || event.missing()) {
            index.appendNull();
            id.appendNull();
            source.appendNull();
            return;
        }
        index.appendBytesRef(new BytesRef(event.index()));
        id.appendBytesRef(new BytesRef(event.id()));
        BytesReference sourceRef = event.source();
        if (sourceRef == null) {
            source.appendNull();
        } else {
            source.appendBytesRef(sourceRef.toBytesRef());
        }
    }

    /** Appends the join keys as a single multivalued keyword value (empty {@code by} value when there are none). */
    private static void appendJoinKeys(List<Object> joinKeys, BytesRefBlock.Builder by) {
        if (joinKeys == null || joinKeys.isEmpty()) {
            by.appendNull();
            return;
        }
        if (joinKeys.size() == 1) {
            by.appendBytesRef(new BytesRef(String.valueOf(joinKeys.get(0))));
            return;
        }
        by.beginPositionEntry();
        for (Object key : joinKeys) {
            by.appendBytesRef(new BytesRef(String.valueOf(key)));
        }
        by.endPositionEntry();
    }
}
