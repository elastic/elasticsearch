/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.FilterVectorValues;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * reads from byte-encoded data
 *
 * NOTE: this class was temporarily copied from Lucene to fix a bug in Lucene94HnswVectorsReader.
 * It contains no modifications to the Lucene version.
 */
public class XExpandingVectorValues extends FilterVectorValues {

    private final float[] value;

    /** @param in the wrapped values */
    protected XExpandingVectorValues(VectorValues in) {
        super(in);
        value = new float[in.dimension()];
    }

    @Override
    public float[] vectorValue() throws IOException {
        BytesRef binaryValue = binaryValue();
        byte[] bytes = binaryValue.bytes;
        for (int i = 0, j = binaryValue.offset; i < value.length; i++, j++) {
            value[i] = bytes[j];
        }
        return value;
    }
}
