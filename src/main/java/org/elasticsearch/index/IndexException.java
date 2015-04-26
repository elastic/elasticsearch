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

package org.elasticsearch.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public class IndexException extends ElasticsearchException {

    private final Index index;

    public IndexException(Index index, String msg) {
        this(index, msg, null);
    }

    protected IndexException(Index index, String msg, Throwable cause) {
        super(msg, cause);
        this.index = index;
    }

    public Index index() {
        return index;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (index != null) {
            builder.field("index", index.getName());
        }
        super.innerToXContent(builder, params);
    }

    @Override
    public String toString() {
        return "[" + (index == null ? "_na" : index.name()) + "] " + getMessage();
    }
}
