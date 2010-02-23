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

package org.elasticsearch.index.query.json;

import org.elasticsearch.index.query.QueryBuilderException;
import org.elasticsearch.util.io.FastCharArrayWriter;
import org.elasticsearch.util.json.BinaryJsonBuilder;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.json.StringJsonBuilder;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class BaseJsonQueryBuilder implements JsonQueryBuilder {

    @Override public String buildAsString() throws QueryBuilderException {
        try {
            StringJsonBuilder builder = JsonBuilder.stringJsonBuilder();
            toJson(builder, EMPTY_PARAMS);
            return builder.string();
        } catch (Exception e) {
            throw new QueryBuilderException("Failed to build query", e);
        }
    }

    @Override public FastCharArrayWriter buildAsUnsafeChars() throws QueryBuilderException {
        try {
            StringJsonBuilder builder = JsonBuilder.stringJsonBuilder();
            toJson(builder, EMPTY_PARAMS);
            return builder.unsafeChars();
        } catch (Exception e) {
            throw new QueryBuilderException("Failed to build query", e);
        }
    }

    @Override public byte[] buildAsBytes() throws QueryBuilderException {
        try {
            BinaryJsonBuilder builder = JsonBuilder.binaryJsonBuilder();
            toJson(builder, EMPTY_PARAMS);
            return builder.copiedBytes();
        } catch (Exception e) {
            throw new QueryBuilderException("Failed to build query", e);
        }
    }

    @Override public void toJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject();
        doJson(builder, params);
        builder.endObject();
    }

    protected abstract void doJson(JsonBuilder builder, Params params) throws IOException;
}
