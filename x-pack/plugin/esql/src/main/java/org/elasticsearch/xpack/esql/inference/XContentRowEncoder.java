/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.BytesRefStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.PositionToXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Encodes rows into an XContent format (JSON,YAML,...) for further processing.
 * Extracted columns can be specified using {@link ExpressionEvaluator}
 */
public class XContentRowEncoder implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(XContentRowEncoder.class);

    private final XContentType xContentType;
    private final ZoneId zoneId;
    private final BlockFactory blockFactory;
    private final ColumnInfoImpl[] columnsInfo;
    private final ExpressionEvaluator[] fieldsValueEvaluators;

    /**
     * Creates a factory for YAML XContent row encoding.
     *
     * @param fieldsEvaluatorFactories A map of column information to expression evaluators.
     * @return A Factory instance for creating YAML row encoder for the specified column.
     */
    public static Factory yamlRowEncoderFactory(ZoneId zoneId, Map<ColumnInfoImpl, ExpressionEvaluator.Factory> fieldsEvaluatorFactories) {
        return new Factory(XContentType.YAML, zoneId, fieldsEvaluatorFactories);
    }

    private XContentRowEncoder(
        XContentType xContentType,
        ZoneId zoneId,
        BlockFactory blockFactory,
        ColumnInfoImpl[] columnsInfo,
        ExpressionEvaluator[] fieldsValueEvaluators
    ) {
        assert columnsInfo.length == fieldsValueEvaluators.length;
        this.xContentType = xContentType;
        this.zoneId = zoneId;
        this.blockFactory = blockFactory;
        this.columnsInfo = columnsInfo;
        this.fieldsValueEvaluators = fieldsValueEvaluators;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(fieldsValueEvaluators);
    }

    /**
     * Process the provided Page and encode its rows into a BytesRefBlock containing XContent-formatted rows.
     *
     * @param page The input Page containing row data.
     * @return A BytesRefBlock containing the encoded rows.
     */
    @Override
    public BytesRefBlock eval(Page page) {
        Block[] fieldValueBlocks = new Block[fieldsValueEvaluators.length];

        try (
            BytesRefStreamOutput outputStream = new BytesRefStreamOutput();
            BytesRefBlock.Builder outputBlockBuilder = blockFactory.newBytesRefBlockBuilder(page.getPositionCount())
        ) {

            PositionToXContent[] toXContents = new PositionToXContent[fieldsValueEvaluators.length];
            for (int b = 0; b < fieldValueBlocks.length; b++) {
                fieldValueBlocks[b] = fieldsValueEvaluators[b].eval(page);
                toXContents[b] = PositionToXContent.positionToXContent(columnsInfo[b], fieldValueBlocks[b], zoneId, new BytesRef());
            }

            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                try (XContentBuilder xContentBuilder = XContentFactory.contentBuilder(xContentType, outputStream)) {

                    xContentBuilder.startObject();
                    boolean hasNullsOnly = true;
                    for (int i = 0; i < fieldValueBlocks.length; i++) {
                        String fieldName = columnsInfo[i].name();
                        Block currentBlock = fieldValueBlocks[i];
                        if (currentBlock.isNull(pos) || currentBlock.getValueCount(pos) < 1) {
                            continue;
                        }
                        hasNullsOnly = false;
                        toXContents[i].positionToXContent(xContentBuilder.field(fieldName), ToXContent.EMPTY_PARAMS, pos);
                    }
                    xContentBuilder.endObject().flush();

                    if (hasNullsOnly) {
                        outputBlockBuilder.appendNull();
                    } else {
                        outputBlockBuilder.appendBytesRef(outputStream.get());
                        outputStream.reset();
                    }
                }
            }

            return outputBlockBuilder.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            Releasables.closeExpectNoException(fieldValueBlocks);
        }
    }

    @Override
    public long baseRamBytesUsed() {
        long baseRamBytesUsed = BASE_RAM_BYTES_USED;
        for (ExpressionEvaluator e : fieldsValueEvaluators) {
            baseRamBytesUsed += e.baseRamBytesUsed();
        }
        return baseRamBytesUsed;
    }

    public List<String> fieldNames() {
        return Arrays.stream(columnsInfo).map(ColumnInfoImpl::name).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "XContentRowEncoder[content_type=[" + xContentType.toString() + "], field_names=" + fieldNames() + "]";
    }

    public static class Factory implements ExpressionEvaluator.Factory {
        private final XContentType xContentType;
        private final ZoneId zoneId;
        private final Map<ColumnInfoImpl, ExpressionEvaluator.Factory> fieldsEvaluatorFactories;

        Factory(XContentType xContentType, ZoneId zoneId, Map<ColumnInfoImpl, ExpressionEvaluator.Factory> fieldsEvaluatorFactories) {
            this.xContentType = xContentType;
            this.zoneId = zoneId;
            this.fieldsEvaluatorFactories = fieldsEvaluatorFactories;
        }

        public XContentRowEncoder get(DriverContext context) {
            return new XContentRowEncoder(xContentType, zoneId, context.blockFactory(), columnsInfo(), fieldsValueEvaluators(context));
        }

        private ColumnInfoImpl[] columnsInfo() {
            return fieldsEvaluatorFactories.keySet().toArray(ColumnInfoImpl[]::new);
        }

        private ExpressionEvaluator[] fieldsValueEvaluators(DriverContext context) {
            return fieldsEvaluatorFactories.values().stream().map(factory -> factory.get(context)).toArray(ExpressionEvaluator[]::new);
        }
    }
}
