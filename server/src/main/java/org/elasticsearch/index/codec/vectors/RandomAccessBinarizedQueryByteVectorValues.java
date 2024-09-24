/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors;

import java.io.IOException;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
public interface RandomAccessBinarizedQueryByteVectorValues {
    float getCentroidDistance(int targetOrd) throws IOException;

    float getLower(int targetOrd) throws IOException;

    float getWidth(int targetOrd) throws IOException;

    float getNormVmC(int targetOrd) throws IOException;

    float getVDotC(int targetOrd) throws IOException;

    int sumQuantizedValues(int targetOrd) throws IOException;

    byte[] vectorValue(int targetOrd) throws IOException;

    int size() throws IOException;

    int dimension();

    RandomAccessBinarizedQueryByteVectorValues copy() throws IOException;
}
