/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesRefStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

public class XContentRowEncoder implements RowEncoder<BytesRefBlock> {
    private final XContentType xContentType;
    private final BlockFactory blockFactory;
    private final String[] fieldNames;
    private final EvalOperator.ExpressionEvaluator[] fieldsValueEvaluators;

    public static Factory yamlRowEncoderFactory(Map<String, EvalOperator.ExpressionEvaluator.Factory> fieldsEvaluatorFactories) {
        return new Factory(XContentType.YAML, fieldsEvaluatorFactories);
    }

    private XContentRowEncoder(
        XContentType xContentType,
        BlockFactory blockFactory,
        String[] fieldNames,
        EvalOperator.ExpressionEvaluator[] fieldsValueEvaluators
    ) {
        assert fieldNames.length == fieldsValueEvaluators.length;
        this.xContentType = xContentType;
        this.blockFactory = blockFactory;
        this.fieldNames = fieldNames;
        this.fieldsValueEvaluators = fieldsValueEvaluators;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(fieldsValueEvaluators);
    }

    @Override
    public BytesRefBlock encodeRows(Page page) {
        Block[] fieldValueBlocks = new Block[fieldsValueEvaluators.length];
        try (
            BytesRefStreamOutput outputStream = new BytesRefStreamOutput();
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(xContentType, outputStream);
            BytesRefBlock.Builder outputBlockBuilder = blockFactory.newBytesRefBlockBuilder(page.getPositionCount());
        ) {
            for (int b = 0; b < fieldValueBlocks.length; b++) {
                fieldValueBlocks[b] = fieldsValueEvaluators[b].eval(page);
            }

            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                xContentBuilder.startObject();
                for (int i = 0; i < fieldValueBlocks.length; i++) {
                    String fieldName = fieldNames[i];
                    Block currentBlock = fieldValueBlocks[i];
                    if (currentBlock.isNull(pos)) {
                        continue;
                    }
                    xContentBuilder.field(fieldName, toYamlValue(BlockUtils.toJavaObject(currentBlock, pos)));
                }
                xContentBuilder.endObject().flush();
                outputBlockBuilder.appendBytesRef(outputStream.get());
                outputStream.reset();
            }

            return outputBlockBuilder.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            Releasables.closeExpectNoException(fieldValueBlocks);
        }
    }

    @Override
    public String toString() {
        return "XContentRowEncoder[content_type=[" + xContentType.toString() + "], field_names=" + List.of(fieldNames) + "]";
    }

    private Object toYamlValue(Object value) {
        try {
            return switch (value) {
                case BytesRef b -> b.utf8ToString();
                case List<?> l -> l.stream().map(this::toYamlValue).toList();
                default -> value;
            };
        } catch (Error | Exception e) {
            // Swallow errors caused by invalid byteref.
            return "";
        }
    }

    public static final class Factory implements RowEncoder.Factory<BytesRefBlock> {
        private final XContentType xContentType;
        private final Map<String, EvalOperator.ExpressionEvaluator.Factory> fieldsEvaluatorFactories;

        private Factory(XContentType xContentType, Map<String, EvalOperator.ExpressionEvaluator.Factory> fieldsEvaluatorFactories) {
            this.xContentType = xContentType;
            this.fieldsEvaluatorFactories = fieldsEvaluatorFactories;
        }

        public RowEncoder<BytesRefBlock> get(DriverContext context) {
            return new XContentRowEncoder(xContentType, context.blockFactory(), fieldNames(), fieldsValueEvaluators(context));
        }

        private String[] fieldNames() {
            return fieldsEvaluatorFactories.keySet().toArray(String[]::new);
        }

        private EvalOperator.ExpressionEvaluator[] fieldsValueEvaluators(DriverContext context) {
            return fieldsEvaluatorFactories.values()
                .stream()
                .map(factory -> factory.get(context))
                .toArray(EvalOperator.ExpressionEvaluator[]::new);
        }
    }
}
