/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * TO_STRING's packed-dimension evaluator. Rendering is dictionary-aware and keeps
 * source-shaped output compatibility separate from packed identity and manipulation.
 */
public final class ToStringFromPackDimEvaluator implements ExpressionEvaluator {
    private final ExpressionEvaluator child;

    private ToStringFromPackDimEvaluator(ExpressionEvaluator child) {
        this.child = child;
    }

    /** Reconstructs source-shaped paths only at the public PromQL JSON compatibility boundary. */
    private static BytesRef encodeSourceRecord(PackDimValue record) throws IOException {
        Map<String, Object> source = Map.of();
        for (int i = 0; i < record.size(); i++) {
            source = set(
                source,
                record.nameAt(i, new BytesRef()).utf8ToString(),
                PackDimValueCodec.decode(record.valueAt(i, new BytesRef()))
            );
        }
        return PackDimValueCodec.encode(source);
    }

    /** Preserves the public source rendering convention: dotted paths nest and null leaves are omitted. */
    private static Map<String, Object> set(Map<?, ?> record, String path, Object value) {
        var result = new TreeMap<String, Object>();
        for (var entry : record.entrySet())
            result.put((String) entry.getKey(), entry.getValue());
        int dot = path.indexOf('.');
        if (record.containsKey(path) || dot < 0) {
            if (value == null) result.remove(path);
            else result.put(path, value);
        } else {
            String parent = path.substring(0, dot);
            Object previous = result.get(parent);
            if (value == null && previous instanceof Map<?, ?> == false) return result;
            Map<String, Object> child = set(previous instanceof Map<?, ?> map ? map : Map.of(), path.substring(dot + 1), value);
            if (child.isEmpty()) result.remove(parent);
            else result.put(parent, child);
        }
        return result;
    }

    /** Builds the packed specialization used by TO_STRING. */
    public record Factory(ExpressionEvaluator.Factory child) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new ToStringFromPackDimEvaluator(child.get(context));
        }
    }

    @Override
    public Block eval(Page page) {
        try (
            var input = (PackDimBlock) child.eval(page);
            var batch = new PackDimBatch(input);
            var builder = input.blockFactory().newBytesRefBlockBuilder(batch.records().getPositionCount())
        ) {
            var records = batch.records();
            var value = new PackDimValue();
            for (int p = 0; p < records.getPositionCount(); p++) {
                builder.appendBytesRef(encodeSourceRecord(records.getPackDim(records.getFirstValueIndex(p), value)));
            }
            try (var rendered = builder.build()) {
                return batch.expand(rendered);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long baseRamBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(ToStringFromPackDimEvaluator.class) + child.baseRamBytesUsed();
    }

    @Override
    public void close() {
        child.close();
    }
}
