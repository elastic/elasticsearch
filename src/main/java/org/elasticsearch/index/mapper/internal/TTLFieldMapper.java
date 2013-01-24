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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AlreadyExpiredException;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeTimeValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

public class TTLFieldMapper extends LongFieldMapper implements InternalMapper, RootMapper {

    public static final String NAME = "_ttl";
    public static final String CONTENT_TYPE = "_ttl";

    public static class Defaults extends LongFieldMapper.Defaults {
        public static final String NAME = TTLFieldMapper.CONTENT_TYPE;

        public static final FieldType TTL_FIELD_TYPE = new FieldType(LongFieldMapper.Defaults.FIELD_TYPE);

        static {
            TTL_FIELD_TYPE.setStored(true);
            TTL_FIELD_TYPE.setIndexed(true);
            TTL_FIELD_TYPE.setTokenized(false);
            TTL_FIELD_TYPE.freeze();
        }

        public static final boolean ENABLED = false;
        public static final long DEFAULT = -1;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, TTLFieldMapper> {

        private boolean enabled = Defaults.ENABLED;
        private long defaultTTL = Defaults.DEFAULT;

        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.TTL_FIELD_TYPE));
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return builder;
        }

        public Builder defaultTTL(long defaultTTL) {
            this.defaultTTL = defaultTTL;
            return builder;
        }

        @Override
        public TTLFieldMapper build(BuilderContext context) {
            return new TTLFieldMapper(fieldType, enabled, defaultTTL, ignoreMalformed(context), provider, fieldDataSettings);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            TTLFieldMapper.Builder builder = new TTLFieldMapper.Builder();
            parseField(builder, builder.name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("default")) {
                    TimeValue ttlTimeValue = nodeTimeValue(fieldNode, null);
                    if (ttlTimeValue != null) {
                        builder.defaultTTL(ttlTimeValue.millis());
                    }
                }
            }
            return builder;
        }
    }

    private boolean enabled;
    private long defaultTTL;

    public TTLFieldMapper() {
        this(new FieldType(Defaults.TTL_FIELD_TYPE), Defaults.ENABLED, Defaults.DEFAULT, Defaults.IGNORE_MALFORMED, null, null);
    }

    protected TTLFieldMapper(FieldType fieldType, boolean enabled, long defaultTTL, Explicit<Boolean> ignoreMalformed,
                             PostingsFormatProvider provider, @Nullable Settings fieldDataSettings) {
        super(new Names(Defaults.NAME, Defaults.NAME, Defaults.NAME, Defaults.NAME), Defaults.PRECISION_STEP,
                Defaults.FUZZY_FACTOR, Defaults.BOOST, fieldType, Defaults.NULL_VALUE, ignoreMalformed,
                provider, null, fieldDataSettings);
        this.enabled = enabled;
        this.defaultTTL = defaultTTL;
    }

    public boolean enabled() {
        return this.enabled;
    }

    public long defaultTTL() {
        return this.defaultTTL;
    }

    // Overrides valueForSearch to display live value of remaining ttl
    @Override
    public Object valueForSearch(Object value) {
        long now;
        SearchContext searchContext = SearchContext.current();
        if (searchContext != null) {
            now = searchContext.nowInMillis();
        } else {
            now = System.currentTimeMillis();
        }
        long val = value(value);
        return val - now;
    }

    // Other implementation for realtime get display
    public Object valueForSearch(long expirationTime) {
        return expirationTime - System.currentTimeMillis();
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) throws IOException, MapperParsingException {
        if (context.sourceToParse().ttl() < 0) { // no ttl has been provided externally
            long ttl;
            if (context.parser().currentToken() == XContentParser.Token.VALUE_STRING) {
                ttl = TimeValue.parseTimeValue(context.parser().text(), null).millis();
            } else {
                ttl = context.parser().longValue();
            }
            if (ttl <= 0) {
                throw new MapperParsingException("TTL value must be > 0. Illegal value provided [" + ttl + "]");
            }
            context.sourceToParse().ttl(ttl);
        }
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected Field innerParseCreateField(ParseContext context) throws IOException, AlreadyExpiredException {
        if (enabled) {
            long ttl = context.sourceToParse().ttl();
            if (ttl <= 0 && defaultTTL > 0) { // no ttl provided so we use the default value
                ttl = defaultTTL;
                context.sourceToParse().ttl(ttl);
            }
            if (ttl > 0) { // a ttl has been provided either externally or in the _source
                long timestamp = context.sourceToParse().timestamp();
                long expire = new Date(timestamp + ttl).getTime();
                long now = System.currentTimeMillis();
                // there is not point indexing already expired doc
                if (now >= expire) {
                    throw new AlreadyExpiredException(context.index(), context.type(), context.id(), timestamp, ttl, now);
                }
                // the expiration timestamp (timestamp + ttl) is set as field
                return new CustomLongNumericField(this, expire, fieldType);
            }
        }
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // if all are defaults, no sense to write it at all
        if (enabled == Defaults.ENABLED && defaultTTL == Defaults.DEFAULT) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (defaultTTL != Defaults.DEFAULT) {
            builder.field("default", defaultTTL);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        TTLFieldMapper ttlMergeWith = (TTLFieldMapper) mergeWith;
        if (!mergeContext.mergeFlags().simulate()) {
            if (ttlMergeWith.defaultTTL != -1) {
                this.defaultTTL = ttlMergeWith.defaultTTL;
            }
        }
    }
}
