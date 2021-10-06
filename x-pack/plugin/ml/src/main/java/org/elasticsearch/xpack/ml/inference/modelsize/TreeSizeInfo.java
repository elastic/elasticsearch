/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.TreeInferenceModel;


import java.io.IOException;
import java.util.Objects;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.ml.inference.modelsize.SizeEstimatorHelper.sizeOfDoubleArray;

public class TreeSizeInfo implements Accountable, ToXContentObject {

    private static final ParseField NUM_NODES = new ParseField("num_nodes");
    private static final ParseField NUM_LEAVES = new ParseField("num_leaves");
    private static final ParseField NUM_CLASSES = new ParseField("num_classes");

    static ConstructingObjectParser<TreeSizeInfo, Void> PARSER = new ConstructingObjectParser<>(
        "tree_size",
        false,
        a -> new TreeSizeInfo((Integer)a[0], a[1] == null ? 0 : (Integer)a[1], a[2] == null ? 0 : (Integer)a[2])
    );
    static {
        PARSER.declareInt(constructorArg(), NUM_LEAVES);
        PARSER.declareInt(optionalConstructorArg(), NUM_NODES);
        PARSER.declareInt(optionalConstructorArg(), NUM_CLASSES);
    }

    public static TreeSizeInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final int numNodes;
    private final int numLeaves;
    private int numClasses;

    TreeSizeInfo(int numLeaves, int numNodes, int numClasses) {
        this.numLeaves = numLeaves;
        this.numNodes = numNodes;
        this.numClasses = numClasses;
    }

    public TreeSizeInfo setNumClasses(int numClasses) {
        this.numClasses = numClasses;
        return this;
    }

    @Override
    public long ramBytesUsed() {
        long size = TreeInferenceModel.SHALLOW_SIZE;
        // Node shallow sizes, covers most information as elements are primitive
        size += NUM_BYTES_ARRAY_HEADER + ((numLeaves + numNodes) * NUM_BYTES_OBJECT_REF);
        size += numLeaves * TreeInferenceModel.LeafNode.SHALLOW_SIZE;
        size += numNodes * TreeInferenceModel.InnerNode.SHALLOW_SIZE;
        // This handles the values within the leaf value array
        int numLeafVals = numClasses <= 2 ? 1 : numClasses;
        size += sizeOfDoubleArray(numLeafVals) * numLeaves;
        return alignObjectSize(size);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_LEAVES.getPreferredName(), numLeaves);
        builder.field(NUM_NODES.getPreferredName(), numNodes);
        builder.field(NUM_CLASSES.getPreferredName(), numClasses);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TreeSizeInfo treeSizeInfo = (TreeSizeInfo) o;
        return numNodes == treeSizeInfo.numNodes &&
            numLeaves == treeSizeInfo.numLeaves &&
            numClasses == treeSizeInfo.numClasses;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numNodes, numLeaves, numClasses);
    }
}
