/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.blob;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

public class CachedBlob implements ToXContent {

    /**
     * Sentinel {@link CachedBlob} indicating that searching the cache index returned an error.
     */
    public static final CachedBlob CACHE_NOT_READY = new CachedBlob(null, null, "CACHE_NOT_READY", null, BytesArray.EMPTY, 0L, 0L);

    /**
     * Sentinel {@link CachedBlob} indicating that the cache index definitely did not contain the requested data.
     */
    public static final CachedBlob CACHE_MISS = new CachedBlob(null, null, "CACHE_MISS", null, BytesArray.EMPTY, 0L, 0L);

    private static final String TYPE = "blob";
    public static final String CREATION_TIME_FIELD = "creation_time";

    private final Instant creationTime;
    private final String repository;
    private final String name;
    private final String path;

    private final BytesReference bytes;
    private final long from;
    private final long to;

    public CachedBlob(Instant creationTime, String repository, String name, String path, BytesReference content, long offset) {
        this(creationTime, repository, name, path, content, offset, offset + (content == null ? 0 : content.length()));
    }

    private CachedBlob(Instant creationTime, String repository, String name, String path, BytesReference content, long from, long to) {
        this.creationTime = creationTime;
        this.repository = repository;
        this.name = name;
        this.path = path;
        this.bytes = content;
        this.from = from;
        this.to = to;
        assert this.to == this.from + this.bytes.length();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        @UpdateForV9(owner = UpdateForV9.Owner.SEARCH_FOUNDATIONS)
        // we can remove the version field when we no longer need to keep compatibility with <8.12
        final int version = Version.CURRENT.id;
        builder.startObject();
        {
            builder.field("type", TYPE);
            builder.field(CREATION_TIME_FIELD, creationTime.toEpochMilli());
            builder.field("version", version);
            builder.field("repository", repository);
            builder.startObject("blob");
            {
                builder.field("name", name);
                builder.field("path", path);
            }
            builder.endObject();
            builder.startObject("data");
            {
                builder.field("content", BytesReference.toBytes(bytes));
                builder.field("length", bytes.length());
                builder.field("from", from);
                builder.field("to", to);
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    public long from() {
        return from;
    }

    public long to() {
        return to;
    }

    public int length() {
        return bytes.length();
    }

    public BytesReference bytes() {
        return bytes;
    }

    public Instant creationTime() {
        return creationTime;
    }

    @SuppressWarnings("unchecked")
    public static CachedBlob fromSource(final Map<String, Object> source) {
        final Long creationTimeEpochMillis = (Long) source.get(CREATION_TIME_FIELD);
        if (creationTimeEpochMillis == null) {
            throw new IllegalStateException("cached blob document does not have the [creation_time] field");
        }
        final String repository = (String) source.get("repository");
        if (repository == null) {
            throw new IllegalStateException("cached blob document does not have the [repository] field");
        }
        final Map<String, ?> blob = (Map<String, ?>) source.get("blob");
        if (blob == null || blob.isEmpty()) {
            throw new IllegalStateException("cached blob document does not have the [blob] object");
        }
        final String name = (String) blob.get("name");
        if (name == null) {
            throw new IllegalStateException("cached blob document does not have the [blob.name] field");
        }
        final String path = (String) blob.get("path");
        if (path == null) {
            throw new IllegalStateException("cached blob document does not have the [blob.path] field");
        }
        final Map<String, ?> data = (Map<String, ?>) source.get("data");
        if (data == null || data.isEmpty()) {
            throw new IllegalStateException("cached blob document does not have the [data] fobjectield");
        }
        final String encodedContent = (String) data.get("content");
        if (encodedContent == null) {
            throw new IllegalStateException("cached blob document does not have the [data.content] field");
        }
        final Integer length = (Integer) data.get("length");
        if (length == null) {
            throw new IllegalStateException("cached blob document does not have the [data.length] field");
        }
        final byte[] content = Base64.getDecoder().decode(encodedContent);
        if (content.length != length) {
            throw new IllegalStateException("cached blob document content length does not match [data.length] field");
        }
        final Number from = (Number) data.get("from");
        if (from == null) {
            throw new IllegalStateException("cached blob document does not have the [data.from] field");
        }
        final Number to = (Number) data.get("to");
        if (to == null) {
            throw new IllegalStateException("cached blob document does not have the [data.to] field");
        }
        // TODO add exhaustive verifications (from/to/content.length, version supported, id == recomputed id etc)
        return new CachedBlob(
            Instant.ofEpochMilli(creationTimeEpochMillis),
            repository,
            name,
            path,
            new BytesArray(content),
            from.longValue(),
            to.longValue()
        );
    }

    @Override
    public String toString() {
        return "CachedBlob ["
            + "creationTime="
            + creationTime
            + ", repository='"
            + repository
            + '\''
            + ", name='"
            + name
            + '\''
            + ", path='"
            + path
            + '\''
            + ", from="
            + from
            + ", to="
            + to
            + ']';
    }
}
