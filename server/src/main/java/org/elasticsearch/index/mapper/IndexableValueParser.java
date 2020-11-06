/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Consumer;

public abstract class IndexableValueParser {

    public abstract void parseAndIndex(XContentParser parser, Consumer<IndexableValue> indexer) throws IOException;

    public final IndexableValue parse(XContentParser parser) throws IOException {
        IndexableValue[] holder = new IndexableValue[1];
        parseAndIndex(parser, value -> holder[0] = value);
        return holder[0];
    }

    public final IndexableValue parseObject(Object object) throws IOException {
        try (XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            Collections.singletonMap("placeholder", object), null)) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return parse(parser);
        }
    }

    public static final IndexableValueParser NO_OP = new IndexableValueParser() {
        @Override
        public void parseAndIndex(XContentParser parser, Consumer<IndexableValue> indexer) { }
    };

}
