/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.json;

import org.elasticsearch.util.gcommon.collect.Lists;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class JsonMergeContext {

    private final JsonDocumentMapper documentMapper;

    private final DocumentMapper.MergeFlags mergeFlags;

    private final List<String> mergeConflicts = Lists.newArrayList();

    public JsonMergeContext(JsonDocumentMapper documentMapper, DocumentMapper.MergeFlags mergeFlags) {
        this.documentMapper = documentMapper;
        this.mergeFlags = mergeFlags;
    }

    public JsonDocumentMapper docMapper() {
        return documentMapper;
    }

    public DocumentMapper.MergeFlags mergeFlags() {
        return mergeFlags;
    }

    public void addConflict(String mergeFailure) {
        mergeConflicts.add(mergeFailure);
    }

    public boolean hasConflicts() {
        return !mergeConflicts.isEmpty();
    }

    public String[] buildConflicts() {
        return mergeConflicts.toArray(new String[mergeConflicts.size()]);
    }
}
