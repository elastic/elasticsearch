/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.id.simple;

import gnu.trove.impl.hash.TObjectHash;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.trove.ExtTObjectIntHasMap;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;

/**
 *
 */
public class SimpleIdReaderTypeCache implements IdReaderTypeCache {

    private final String type;

    private final ExtTObjectIntHasMap<HashedBytesArray> idToDoc;

    private final HashedBytesArray[] docIdToId;

    private final HashedBytesArray[] parentIdsValues;

    private final int[] parentIdsOrdinals;

    private long sizeInBytes = -1;

    public SimpleIdReaderTypeCache(String type, ExtTObjectIntHasMap<HashedBytesArray> idToDoc, HashedBytesArray[] docIdToId,
                                   HashedBytesArray[] parentIdsValues, int[] parentIdsOrdinals) {
        this.type = type;
        this.idToDoc = idToDoc;
        this.docIdToId = docIdToId;
        this.idToDoc.trimToSize();
        this.parentIdsValues = parentIdsValues;
        this.parentIdsOrdinals = parentIdsOrdinals;
    }

    public String type() {
        return this.type;
    }

    public HashedBytesArray parentIdByDoc(int docId) {
        return parentIdsValues[parentIdsOrdinals[docId]];
    }

    public int docById(HashedBytesArray uid) {
        return idToDoc.get(uid);
    }

    public HashedBytesArray idByDoc(int docId) {
        return docIdToId[docId];
    }

    public long sizeInBytes() {
        if (sizeInBytes == -1) {
            sizeInBytes = computeSizeInBytes();
        }
        return sizeInBytes;
    }

    /**
     * Returns an already stored instance if exists, if not, returns null;
     */
    public HashedBytesArray canReuse(HashedBytesArray id) {
        return idToDoc.key(id);
    }

    long computeSizeInBytes() {
        long sizeInBytes = 0;
        // Ignore type field
        //  sizeInBytes += ((type.length() * RamUsage.NUM_BYTES_CHAR) + (3 * RamUsage.NUM_BYTES_INT)) + RamUsage.NUM_BYTES_OBJECT_HEADER;
        sizeInBytes += RamUsage.NUM_BYTES_ARRAY_HEADER + (idToDoc._valuesSize() * RamUsage.NUM_BYTES_INT);
        for (Object o : idToDoc._set) {
            if (o == TObjectHash.FREE || o == TObjectHash.REMOVED) {
                sizeInBytes += RamUsage.NUM_BYTES_OBJECT_REF;
            } else {
                HashedBytesArray bytesArray = (HashedBytesArray) o;
                sizeInBytes += RamUsage.NUM_BYTES_OBJECT_HEADER + (bytesArray.length() + RamUsage.NUM_BYTES_INT);
            }
        }

        // The docIdToId array contains references to idToDoc for this segment or other segments, so we can use OBJECT_REF
        sizeInBytes += RamUsage.NUM_BYTES_ARRAY_HEADER + (RamUsage.NUM_BYTES_OBJECT_REF * docIdToId.length);
        for (HashedBytesArray bytesArray : parentIdsValues) {
            if (bytesArray == null) {
                sizeInBytes += RamUsage.NUM_BYTES_OBJECT_REF;
            } else {
                sizeInBytes += RamUsage.NUM_BYTES_OBJECT_HEADER + (bytesArray.length() + RamUsage.NUM_BYTES_INT);
            }
        }
        sizeInBytes += RamUsage.NUM_BYTES_ARRAY_HEADER + (RamUsage.NUM_BYTES_INT * parentIdsOrdinals.length);

        return sizeInBytes;
    }

}
