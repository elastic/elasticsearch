/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.transforms;

import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.Logger;

/**
 * Abstract transform class.
 * Instances are created with maps telling it which field(s)
 * to read from in the input array and where to write to.
 * The read/write area is passed in the {@linkplain #transform(String[][])}
 * function.
 * <p>
 * Some transforms may fail and we will continue processing for
 * others a failure is terminal meaning the record should not be
 * processed further
 */
public abstract class Transform {
    /**
     * OK means the transform was successful,
     * FAIL means the transform failed but it's ok to continue processing
     * EXCLUDE means the no further processing should take place and the record discarded
     */
    public enum TransformResult {
        OK, FAIL, EXCLUDE
    }

    public static class TransformIndex {
        public final int array;
        public final int index;

        public TransformIndex(int a, int b) {
            this.array = a;
            this.index = b;
        }

        @Override
        public int hashCode() {
            return Objects.hash(array, index);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TransformIndex other = (TransformIndex) obj;
            return Objects.equals(this.array, other.array)
                    && Objects.equals(this.index, other.index);
        }
    }

    protected final Logger logger;
    protected final List<TransformIndex> readIndexes;
    protected final List<TransformIndex> writeIndexes;

    /**
     * @param readIndexes  Read inputs from these indexes
     * @param writeIndexes Outputs are written to these indexes
     * @param logger        Transform results go into these indexes
     */
    public Transform(List<TransformIndex> readIndexes, List<TransformIndex> writeIndexes, Logger logger) {
        this.logger = logger;

        this.readIndexes = readIndexes;
        this.writeIndexes = writeIndexes;
    }

    /**
     * The indexes for the inputs
     */
    public final List<TransformIndex> getReadIndexes() {
        return readIndexes;
    }

    /**
     * The write output indexes
     */
    public final List<TransformIndex> getWriteIndexes() {
        return writeIndexes;
    }

    /**
     * Transform function.
     * The read write array of arrays area typically contains an input array,
     * scratch area array and the output array. The scratch area is used in the
     * case where the transform is chained so reads/writes to an intermediate area
     */
    public abstract TransformResult transform(String[][] readWriteArea)
            throws TransformException;
}
