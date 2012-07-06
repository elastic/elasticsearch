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

import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.trove.ExtTObjectIntHasMap;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;

/**
 *
 */
public class SimpleIdReaderTypeCache implements IdReaderTypeCache {

    private final String type;

    private final ExtTObjectIntHasMap<HashedBytesArray> idToDoc;

    private final HashedBytesArray[] parentIdsValues;

    private final int[] parentIdsOrdinals;

    public SimpleIdReaderTypeCache(String type, ExtTObjectIntHasMap<HashedBytesArray> idToDoc,
                                   HashedBytesArray[] parentIdsValues, int[] parentIdsOrdinals) {
        this.type = type;
        this.idToDoc = idToDoc;
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

    public int docById(HashedBytesArray id) {
        return idToDoc.get(id);
    }

    /**
     * Returns an already stored instance if exists, if not, returns null;
     */
    public HashedBytesArray canReuse(HashedBytesArray id) {
        return idToDoc.key(id);
    }
}
