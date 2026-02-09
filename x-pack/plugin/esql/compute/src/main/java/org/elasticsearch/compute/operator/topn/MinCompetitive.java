/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;

import java.util.List;

public class MinCompetitive {
    private final BlockFactory blockFactory;
    private final List<ElementType> elementTypes;
    private final List<TopNEncoder> encoders;
    private final List<TopNOperator.SortOrder> sortOrders;
    private final TopNOperator.Row row;

    MinCompetitive(
        BlockFactory blockFactory,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<TopNOperator.SortOrder> sortOrders,
        TopNOperator.Row row
    ) {
        this.blockFactory = blockFactory;
        this.elementTypes = elementTypes;
        this.encoders = encoders;
        this.sortOrders = sortOrders;
        this.row = row;
    }

    public TopNOperator.SortOrder firstSortOrder() {
        return sortOrders.getFirst();
    }

    /**
     * Build a {@link Block} containing the sort key of the min competitive row.
     */
    public Block firstSortKey() {
        TopNOperator.SortOrder sortOrder = sortOrders.getFirst();
        BytesRef view = row.keys.bytesRefView();
        if (view.bytes[view.offset] == sortOrder.nul()) {
            return blockFactory.newConstantNullBlock(1);
        }
        BytesRef shallow = new BytesRef();
        shallow.bytes = view.bytes;
        shallow.offset = view.offset + 1;
        shallow.length = view.length - 1;

        TopNEncoder encoder = encoders.get(sortOrder.channel()).toUnsortable();
        if (encoder.decodeMutatesBytes()) {
            throw new IllegalStateException("can't mutate bytes on read");
        }

        try (
            ResultBuilder builder = ResultBuilder.resultBuilderFor(blockFactory, elementTypes.get(sortOrder.channel()), encoder, true, 1)
        ) {

            builder.decodeKey(shallow);
            view = row.values.bytesRefView();
            shallow.bytes = view.bytes;
            shallow.offset = view.offset;
            shallow.length = view.length;
            builder.decodeValue(shallow);
            return builder.build();
        }
    }
}
