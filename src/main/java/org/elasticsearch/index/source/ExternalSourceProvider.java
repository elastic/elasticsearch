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

import java.io.IOException;

/**
 *
 */
public interface ExternalSourceProvider {

    /**
     * Name of the source provider
     * @return name of the source provider
     */
    String name();

    /**
     * Dehydrates source
     *
     * Implementations of this method should remove everything from the source that is
     * not necessary to restore the source later. If source can be restored based on type
     * and id, this method should return emptyMap().
     *
     * @param type record type
     * @param id record id
     * @param source original source
     * @param sourceOffset original source offset
     * @param sourceLength original source length
     * @return dehydrated source
     * @throws IOException exception
     */
    BytesHolder dehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) throws IOException;

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

}
