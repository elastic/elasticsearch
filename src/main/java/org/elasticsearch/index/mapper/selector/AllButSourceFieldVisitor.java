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

package org.elasticsearch.index.mapper.selector;

import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.common.lucene.document.MultipleFieldsVisitor;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;

import java.io.IOException;

/**
 * A field selector that loads all fields except the source field.
 */
public class AllButSourceFieldVisitor extends MultipleFieldsVisitor {

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.NO;
        }
        return super.needsField(fieldInfo);
    }

    @Override
    public String toString() {
        return "all_but_source";
    }
}
