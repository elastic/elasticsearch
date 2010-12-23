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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.util.concurrent.ThreadSafe;

/**
 * A mapper that maps the actual source of a generated document.
 *
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public interface SourceFieldMapper extends FieldMapper<byte[]>, InternalMapper {

    public final String NAME = "_source";

    /**
     * Returns <tt>true</tt> if the source field mapper is enabled or not.
     */
    boolean enabled();

    /**
     * Returns the native source value, if its compressed, then the compressed value is returned.
     */
    byte[] nativeValue(Fieldable field);

    byte[] value(Document document);

    /**
     * A field selector that loads just the source field.
     */
    FieldSelector fieldSelector();
}
