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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public abstract class ParsingExternalSourceProvider implements ExternalSourceProvider {
    
    private String name;

    protected ParsingExternalSourceProvider(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public BytesHolder dehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) throws IOException {
        Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(source, sourceOffset, sourceLength, true);
        Map<String, Object> parsedSource = dehydrateSource(type, id, mapTuple.v2());
        if(parsedSource != null) {
            CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
            try {
                StreamOutput streamOutput = cachedEntry.cachedBytes();
                XContentBuilder builder = XContentFactory.contentBuilder(mapTuple.v1(), streamOutput).map(parsedSource);
                builder.close();
                return new BytesHolder(cachedEntry.bytes().copiedByteArray());
            } finally {
                CachedStreamOutput.pushEntry(cachedEntry);
            }
        } else {
            return null;
        }
    }

    protected abstract Map<String, Object> dehydrateSource(String type, String id, Map<String, Object> source) throws IOException;

}
