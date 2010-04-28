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

package org.elasticsearch.index.mapper.xcontent;

import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.util.concurrent.NotThreadSafe;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.xcontent.ToXContent;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public interface XContentMapper extends ToXContent {

    public static final XContentMapper[] EMPTY_ARRAY = new XContentMapper[0];

    @NotThreadSafe
    public static class BuilderContext {
        private final ContentPath contentPath;

        public BuilderContext(ContentPath contentPath) {
            this.contentPath = contentPath;
        }

        public ContentPath path() {
            return this.contentPath;
        }
    }

    @NotThreadSafe
    public static abstract class Builder<T extends Builder, Y extends XContentMapper> {

        protected String name;

        protected T builder;

        protected Builder(String name) {
            this.name = name;
        }

        public abstract Y build(BuilderContext context);
    }

    String name();

    void parse(ParseContext context) throws IOException;

    void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException;

    void traverse(FieldMapperListener fieldMapperListener);
}
