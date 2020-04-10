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
package org.elasticsearch.client.core;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Paging parameters for GET requests
 */
public class PageParams implements ToXContentObject {

    public static final ParseField PAGE = new ParseField("page");
    public static final ParseField FROM = new ParseField("from");
    public static final ParseField SIZE = new ParseField("size");

    public static final ConstructingObjectParser<PageParams, Void> PARSER = new ConstructingObjectParser<>(PAGE.getPreferredName(),
            a -> new PageParams((Integer) a[0], (Integer) a[1]));

    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), FROM);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), SIZE);
    }

    private final Integer from;
    private final Integer size;

    /**
     * Constructs paging parameters
     * @param from skips the specified number of items. When {@code null} the default value will be used.
     * @param size specifies the maximum number of items to obtain. When {@code null} the default value will be used.
     */
    public PageParams(@Nullable Integer from, @Nullable Integer size) {
        this.from = from;
        this.size = size;
    }

    public Integer getFrom() {
        return from;
    }

    public Integer getSize() {
        return size;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (from != null) {
            builder.field(FROM.getPreferredName(), from);
        }
        if (size != null) {
            builder.field(SIZE.getPreferredName(), size);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, size);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PageParams other = (PageParams) obj;
        return Objects.equals(from, other.from) &&
                Objects.equals(size, other.size);
    }

}
