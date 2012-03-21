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

package org.elasticsearch.index.source.field;

import com.google.common.base.Objects;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.lzf.LZF;
import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.source.SourceProvider;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DefaultSourceProvider implements SourceProvider {

    public static class Defaults {
        public static final String NAME = "default";
        public static final long COMPRESS_THRESHOLD = -1;
        public static final String FORMAT = null; // default format is to use the one provided
        public static final String[] INCLUDES = Strings.EMPTY_ARRAY;
        public static final String[] EXCLUDES = Strings.EMPTY_ARRAY;
    }

    private Boolean compress;

    private long compressThreshold;

    private String[] includes;

    private String[] excludes;

    private String format;

    private XContentType formatContentType;

    public DefaultSourceProvider() {
        this(Defaults.FORMAT, null, -1, Defaults.INCLUDES, Defaults.EXCLUDES);
    }

    protected DefaultSourceProvider(String format, Boolean compress, long compressThreshold, String[] includes, String[] excludes) {
        this.compress = compress;
        this.compressThreshold = compressThreshold;
        this.includes = includes;
        this.excludes = excludes;
        this.format = format;
        this.formatContentType = format == null ? null : XContentType.fromRestContentType(format);
    }

    @Override public String name() {
        return Defaults.NAME;
    }

    @Override public BytesHolder dehydrateSource(ParseContext context) throws IOException {
        byte[] data = context.source();
        int dataOffset = context.sourceOffset();
        int dataLength = context.sourceLength();

        boolean filtered = includes.length > 0 || excludes.length > 0;
        if (filtered) {
            // we don't update the context source if we filter, we want to keep it as is...

            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(data, dataOffset, dataLength, true);
            Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), includes, excludes);
            CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
            StreamOutput streamOutput;
            if (compress != null && compress && (compressThreshold == -1 || dataLength > compressThreshold)) {
                streamOutput = cachedEntry.cachedLZFBytes();
            } else {
                streamOutput = cachedEntry.cachedBytes();
            }
            XContentType contentType = formatContentType;
            if (contentType == null) {
                contentType = mapTuple.v1();
            }
            XContentBuilder builder = XContentFactory.contentBuilder(contentType, streamOutput).map(filteredSource);
            builder.close();

            data = cachedEntry.bytes().copiedByteArray();
            dataOffset = 0;
            dataLength = data.length;

            CachedStreamOutput.pushEntry(cachedEntry);
        } else if (compress != null && compress && !LZF.isCompressed(data, dataOffset, dataLength)) {
            if (compressThreshold == -1 || dataLength > compressThreshold) {
                CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
                try {
                    XContentType contentType = XContentFactory.xContentType(data, dataOffset, dataLength);
                    if (formatContentType != null && formatContentType != contentType) {
                        XContentBuilder builder = XContentFactory.contentBuilder(formatContentType, cachedEntry.cachedLZFBytes());
                        builder.copyCurrentStructure(XContentFactory.xContent(contentType).createParser(data, dataOffset, dataLength));
                        builder.close();
                    } else {
                        LZFStreamOutput streamOutput = cachedEntry.cachedLZFBytes();
                        streamOutput.writeBytes(data, dataOffset, dataLength);
                        streamOutput.flush();
                    }
                    // we copy over the byte array, since we need to push back the cached entry
                    // TODO, we we had a handle into when we are done with parsing, then we push back then and not copy over bytes
                    data = cachedEntry.bytes().copiedByteArray();
                    dataOffset = 0;
                    dataLength = data.length;
                    // update the data in the context, so it can be compressed and stored compressed outside...
                    context.source(data, dataOffset, dataLength);
                } finally {
                    CachedStreamOutput.pushEntry(cachedEntry);
                }
            }
        } else if (formatContentType != null) {
            // see if we need to convert the content type
            if (LZF.isCompressed(data, dataOffset, dataLength)) {
                BytesStreamInput siBytes = new BytesStreamInput(data, dataOffset, dataLength, false);
                LZFStreamInput siLzf = CachedStreamInput.cachedLzf(siBytes);
                XContentType contentType = XContentFactory.xContentType(siLzf);
                siLzf.resetToBufferStart();
                if (contentType != formatContentType) {
                    // we need to reread and store back, compressed....
                    CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
                    try {
                        LZFStreamOutput streamOutput = cachedEntry.cachedLZFBytes();
                        XContentBuilder builder = XContentFactory.contentBuilder(formatContentType, streamOutput);
                        builder.copyCurrentStructure(XContentFactory.xContent(contentType).createParser(siLzf));
                        builder.close();
                        data = cachedEntry.bytes().copiedByteArray();
                        dataOffset = 0;
                        dataLength = data.length;
                        // update the data in the context, so we store it in the translog in this format
                        context.source(data, dataOffset, dataLength);
                    } finally {
                        CachedStreamOutput.pushEntry(cachedEntry);
                    }
                }
            } else {
                XContentType contentType = XContentFactory.xContentType(data, dataOffset, dataLength);
                if (contentType != formatContentType) {
                    // we need to reread and store back
                    // we need to reread and store back, compressed....
                    CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
                    try {
                        XContentBuilder builder = XContentFactory.contentBuilder(formatContentType, cachedEntry.cachedBytes());
                        builder.copyCurrentStructure(XContentFactory.xContent(contentType).createParser(data, dataOffset, dataLength));
                        builder.close();
                        data = cachedEntry.bytes().copiedByteArray();
                        dataOffset = 0;
                        dataLength = data.length;
                        // update the data in the context, so we store it in the translog in this format
                        context.source(data, dataOffset, dataLength);
                    } finally {
                        CachedStreamOutput.pushEntry(cachedEntry);
                    }
                }
            }
        }
        return new BytesHolder(data, dataOffset, dataLength);
    }

    @Override public BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) {
        // TODO: Should we decompress here?
        return new BytesHolder(source, sourceOffset, sourceLength);
    }

    public static class Builder {

        private long compressThreshold = Defaults.COMPRESS_THRESHOLD;

        private Boolean compress = null;

        private String format = Defaults.FORMAT;

        private String[] includes = Defaults.INCLUDES;
        private String[] excludes = Defaults.EXCLUDES;

        public Builder compress(boolean compress) {
            this.compress = compress;
            return this;
        }

        public Builder compressThreshold(long compressThreshold) {
            this.compressThreshold = compressThreshold;
            return this;
        }

        public Builder format(String format) {
            this.format = format;
            return this;
        }

        public Builder includes(String[] includes) {
            this.includes = includes;
            return this;
        }

        public Builder excludes(String[] excludes) {
            this.excludes = excludes;
            return this;
        }

        public DefaultSourceProvider build() {
            return new DefaultSourceProvider(format, compress, compressThreshold, includes, excludes);
        }

    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!Objects.equal(format, Defaults.FORMAT)) {
            builder.field("format", format);
        }
        if (compress != null) {
            builder.field("compress", compress);
        }
        if (compressThreshold != -1) {
            builder.field("compress_threshold", new ByteSizeValue(compressThreshold).toString());
        }
        if (includes.length > 0) {
            builder.field("includes", includes);
        }
        if (excludes.length > 0) {
            builder.field("excludes", excludes);
        }
        return builder;
    }

    @Override public void merge(SourceProvider mergeWith, MergeContext mergeContext) {
        if (mergeWith instanceof DefaultSourceProvider) {
            DefaultSourceProvider sourceProviderMergeWith = (DefaultSourceProvider) mergeWith;
            if (sourceProviderMergeWith.compress != null) {
                this.compress = sourceProviderMergeWith.compress;
            }
            if (sourceProviderMergeWith.compressThreshold != -1) {
                this.compressThreshold = sourceProviderMergeWith.compressThreshold;
            }
        }
    }
}
