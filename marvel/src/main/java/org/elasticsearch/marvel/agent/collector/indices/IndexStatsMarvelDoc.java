/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class IndexStatsMarvelDoc extends MarvelDoc<IndexStatsMarvelDoc> {

    private final String index;
    private final Docs docs;
    private final Store store;
    private final Indexing indexing;

    public IndexStatsMarvelDoc(String clusterName, String type, long timestamp,
                               String index, Docs docs, Store store, Indexing indexing) {
        super(clusterName, type, timestamp);
        this.index = index;
        this.docs = docs;
        this.store = store;
        this.indexing = indexing;
    }

    @Override
    public IndexStatsMarvelDoc payload() {
        return this;
    }

    public String getIndex() {
        return index;
    }

    public Docs getDocs() {
        return docs;
    }

    public Store getStore() {
        return store;
    }

    public Indexing getIndexing() {
        return indexing;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.toXContent(builder, params);
        builder.field(Fields.INDEX, index);
        if (docs != null) {
            docs.toXContent(builder, params);
        }
        if (store != null) {
            store.toXContent(builder, params);
        }
        if (indexing != null) {
            indexing.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public static IndexStatsMarvelDoc createMarvelDoc(String clusterName, String type, long timestamp,
                                                 String index, long docsCount, long storeSizeInBytes, long storeThrottleTimeInMillis, long indexingThrottleTimeInMillis) {
        return new IndexStatsMarvelDoc(clusterName, type, timestamp, index,
                                    new Docs(docsCount),
                                    new Store(storeSizeInBytes, storeThrottleTimeInMillis),
                                    new Indexing(indexingThrottleTimeInMillis));
    }

    static final class Fields {
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
    }

    static class Docs implements ToXContent {

        private final long count;

        Docs(long count) {
            this.count = count;
        }

        public long getCount() {
            return count;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.DOCS);
            builder.field(Fields.COUNT, count);
            builder.endObject();
            return builder;
        }

        static final class Fields {
            static final XContentBuilderString DOCS = new XContentBuilderString("docs");
            static final XContentBuilderString COUNT = new XContentBuilderString("count");
        }
    }

    static class Store implements ToXContent {

        private final long sizeInBytes;
        private final long throttleTimeInMillis;

        public Store(long sizeInBytes, long throttleTimeInMillis) {
            this.sizeInBytes = sizeInBytes;
            this.throttleTimeInMillis = throttleTimeInMillis;
        }

        public long getSizeInBytes() {
            return sizeInBytes;
        }

        public long getThrottleTimeInMillis() {
            return throttleTimeInMillis;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.STORE);
            builder.field(Fields.SIZE_IN_BYTES, sizeInBytes);
            builder.timeValueField(Fields.THROTTLE_TIME_IN_MILLIS, Fields.THROTTLE_TIME, new TimeValue(throttleTimeInMillis, TimeUnit.MILLISECONDS));
            builder.endObject();
            return builder;
        }

        static final class Fields {
            static final XContentBuilderString STORE = new XContentBuilderString("store");
            static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");
            static final XContentBuilderString THROTTLE_TIME = new XContentBuilderString("throttle_time");
            static final XContentBuilderString THROTTLE_TIME_IN_MILLIS = new XContentBuilderString("throttle_time_in_millis");
        }
    }

    static class Indexing implements ToXContent {

        private final long throttleTimeInMillis;

        public Indexing(long throttleTimeInMillis) {
            this.throttleTimeInMillis = throttleTimeInMillis;
        }

        public long getThrottleTimeInMillis() {
            return throttleTimeInMillis;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.INDEXING);
            builder.timeValueField(Fields.THROTTLE_TIME_IN_MILLIS, Fields.THROTTLE_TIME, new TimeValue(throttleTimeInMillis, TimeUnit.MILLISECONDS));
            builder.endObject();
            return builder;
        }

        static final class Fields {
            static final XContentBuilderString INDEXING = new XContentBuilderString("indexing");
            static final XContentBuilderString THROTTLE_TIME = new XContentBuilderString("throttle_time");
            static final XContentBuilderString THROTTLE_TIME_IN_MILLIS = new XContentBuilderString("throttle_time_in_millis");
        }
    }
}

