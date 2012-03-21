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

package org.elasticsearch.index.source;

import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;

/**
 *
 */
public interface SourceProvider extends ToXContent {

    /**
     * Canonical name of the source provider
     * @return name of the source provider
     */
    String name();

    /**
     * Dehydrates source
     *
     * Implementations of this method should remove everything from the source that is
     * not necessary to restore the source later. If source can be restored based on type
     * and id, this method should return BytesHolder#EMTPY to indicate that source is still
     * present. Alternatively, it can return
     *
     * new BytesHolder(context.source(), context.offset(), context.length())
     *
     * if source shouldn't be changed or null if source shouldn't be stored.
     *
     * The original source can be found in context.source().
     *
     * @param context parse context
     * @return dehydrated source
     * @throws IOException exception
     */
    BytesHolder dehydrateSource(ParseContext context) throws IOException;

    /**
     * Rehydrates source
     *
     * Implementations of this method should restore the source based on type, id and dehydrated
     * source.
     *
     * @param type record type
     * @param id record id
     * @param source dehydrated source
     * @param sourceOffset dehydrated source offset
     * @param sourceLength dehydrated source length
     * @return rehydrated source
     */
    BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength);

    void merge(SourceProvider mergeWith, MergeContext mergeContext);

}
