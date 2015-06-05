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
package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 */
public class UidAndRoutingFieldsVisitor extends FieldsVisitor {

    private String routing;

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (RoutingFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.YES;
        } else if (UidFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.YES;
        }

        return uid != null && routing != null ? Status.STOP : Status.NO;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, byte[] bytes) throws IOException {
        if (RoutingFieldMapper.NAME.equals(fieldInfo.name)) {
            routing = new String(bytes, StandardCharsets.UTF_8);;
        } else {
            super.stringField(fieldInfo, bytes);
        }
    }

    public String routing() {
        return routing;
    }
}
