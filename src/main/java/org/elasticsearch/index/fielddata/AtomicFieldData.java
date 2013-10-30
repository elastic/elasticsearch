/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.fielddata;

/**
 * The thread safe {@link org.apache.lucene.index.AtomicReader} level cache of the data.
 */
public interface AtomicFieldData<Script extends ScriptDocValues> {

    /**
     * If this method returns false, this means that no document has multiple values. However this method may return true even if all
     * documents are single-valued. So this method is useful for performing optimizations when the single-value case makes the problem
     * simpler but cannot be used to actually check whether this instance is multi-valued.
     */
    boolean isMultiValued();

    /**
     * Are the values ordered? (in ascending manner).
     */
    boolean isValuesOrdered();

    /**
     * The number of docs in this field data.
     */
    int getNumDocs();

    /**
     * An upper limit of the number of unique values in this atomic field data.
     */
    long getNumberUniqueValues();

    /**
     * Size (in bytes) of memory used by this field data.
     */
    long getMemorySizeInBytes();

    /**
     * Use a non thread safe (lightweight) view of the values as bytes.
     *
     * @param needsHashes if <code>true</code> the implementation will use pre-build hashes if
     *                    {@link org.elasticsearch.index.fielddata.BytesValues#currentValueHash()} is used. if no hashes
     *                    are used <code>false</code> should be passed instead.
     *
     */
    BytesValues getBytesValues(boolean needsHashes);
    
    /**
     * Returns a "scripting" based values.
     */
    Script getScriptValues();

    /**
     * Close the field data.
     */
    void close();

    interface WithOrdinals<Script extends ScriptDocValues> extends AtomicFieldData<Script> {

        /**
         * Use a non thread safe (lightweight) view of the values as bytes.
         * @param needsHashes
         */
        BytesValues.WithOrdinals getBytesValues(boolean needsHashes);
    }
}
