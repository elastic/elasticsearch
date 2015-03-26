/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.store;

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;

/** 
 * abstract class for verifying what was written.
 * subclasses override {@link #writeByte(byte)} and {@link #writeBytes(byte[], int, int)}
 */
// do NOT optimize this class for performance
public abstract class VerifyingIndexOutput extends FilterIndexOutput {

    /** Sole constructor */
    VerifyingIndexOutput(IndexOutput out) {
        super("VerifyingIndexOutput(out=" + out.toString() + ")", out);
    }
    
    /**
     * Verifies the checksum and compares the written length with the expected file length. This method should be
     * called after all data has been written to this output.
     */
    public abstract void verify() throws IOException;
}
