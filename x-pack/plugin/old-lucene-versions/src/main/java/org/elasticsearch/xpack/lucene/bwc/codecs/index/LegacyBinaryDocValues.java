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
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.index;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * A per-document byte[]
 *
 * @deprecated Use {@link BinaryDocValues} instead.
 */
@Deprecated
public abstract class LegacyBinaryDocValues {

    /** Sole constructor. (For invocation by subclass
     * constructors, typically implicit.) */
    protected LegacyBinaryDocValues() {}

    /** Lookup the value for document.  The returned {@link BytesRef} may be
     * re-used across calls to {@link #get(int)} so make sure to
     * {@link BytesRef#deepCopyOf(BytesRef) copy it} if you want to keep it
     * around. */
    public abstract BytesRef get(int docID);
}
