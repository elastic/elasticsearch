/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ColumnsBlock;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class ColumnsPositionToXContent extends PositionToXContent {
    private final Map<String, PositionToXContent> columns = new TreeMap<>();

    ColumnsPositionToXContent(ColumnInfoImpl columnInfo, Block block, BytesRef scratch) {
        super(block);
        for (Map.Entry<String, ColumnsBlock.RuntimeTypedBlock> c : ((ColumnsBlock) block).columns().entrySet()) {
            ColumnInfoImpl subInfo = new ColumnInfoImpl(columnInfo.name() + "." + c.getKey(), (DataType) c.getValue().type(), null);
            columns.put(c.getKey(), PositionToXContent.positionToXContent(subInfo, c.getValue().block(), scratch));
        }
    }

    @Override
    public XContentBuilder positionToXContent(XContentBuilder builder, ToXContent.Params params, int position) throws IOException {
        builder.startObject();
        for (Map.Entry<String, PositionToXContent> c : columns.entrySet()) {
            if (c.getValue().block.isNull(position) == false) {
                builder.field(c.getKey());
                c.getValue().positionToXContent(builder, params, position);
            }
        }
        return builder.endObject();
    }

    @Override
    protected XContentBuilder valueToXContent(XContentBuilder builder, ToXContent.Params params, int valueIndex) throws IOException {
        throw new IllegalStateException("NOCOMMIT");
    }
}
