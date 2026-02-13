/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ToTDigestTests extends AbstractScalarFunctionTestCase {

    public ToTDigestTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryHistogram(
            suppliers,
            "ToTDigestFromHistogramEvaluator[in=Attribute[channel=0]]",
            DataType.TDIGEST,
            ToTDigestTests::fromHistogram,
            List.of()
        );

        TestCaseSupplier.forUnaryTDigest(
            suppliers,
            // This gets optimized to a no-op
            "Attribute[channel=0]",
            DataType.TDIGEST,
            h -> h,
            List.of()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToTDigest(source, args.getFirst());
    }

    static TDigestHolder fromHistogram(BytesRef in) {
        if (in.length > ByteSizeUnit.MB.toBytes(2)) {
            throw new IllegalArgumentException("Histogram length is greater than 2MB");
        }
        // even though the encoded format is the same, we need to decode here to compute the summary data
        List<Double> centroids = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        ByteArrayStreamInput streamInput = new ByteArrayStreamInput();
        streamInput.reset(in.bytes, in.offset, in.length);
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0;
        long totalCount = 0;
        try {
            while (streamInput.available() > 0) {
                long count = streamInput.readVLong();
                double value = Double.longBitsToDouble(streamInput.readLong());
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value * count;
                totalCount += count;
                centroids.add(value);
                counts.add(count);
            }
            if (totalCount == 0) {
                min = Double.NaN;
                max = Double.NaN;
                sum = Double.NaN;
            }
            return new TDigestHolder(centroids, counts, min, max, sum, totalCount);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
}
