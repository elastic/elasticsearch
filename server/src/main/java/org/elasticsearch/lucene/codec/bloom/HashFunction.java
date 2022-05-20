/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
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
 */
package org.elasticsearch.lucene.codec.bloom;

import org.apache.lucene.util.BytesRef;

/**
 * Base class for hashing functions that can be referred to by name. Subclasses are expected to
 * provide threadsafe implementations of the hash function on the range of bytes referenced in the
 * provided {@link BytesRef}
 *
 * @lucene.experimental
 */
public abstract class HashFunction {

    /**
     * Hashes the contents of the referenced bytes
     *
     * @param bytes the data to be hashed
     * @return the hash of the bytes referenced by bytes.offset and length bytes.length
     */
    public abstract int hash(BytesRef bytes);
}
