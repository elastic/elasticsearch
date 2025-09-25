/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.expectValueToken;

/**
 * A builder for computing a hash from fields in the document source that are part of the
 * {@link org.elasticsearch.cluster.metadata.IndexMetadata#INDEX_ROUTING_PATH}.
 * It is used in the context of {@link IndexRouting.ExtractFromSource.ForRoutingPath} to determine the shard a document should be routed to.
 */
public class RoutingHashBuilder {
    private final List<NameAndHash> hashes = new ArrayList<>();
    private final Predicate<String> isRoutingPath;

    public RoutingHashBuilder(Predicate<String> isRoutingPath) {
        this.isRoutingPath = isRoutingPath;
    }

    public void addMatching(String fieldName, BytesRef string) {
        if (isRoutingPath.test(fieldName)) {
            addHash(fieldName, string);
        }
    }

    /**
     * Only expected to be called for old indices created before
     * {@link IndexVersions#TIME_SERIES_ROUTING_HASH_IN_ID} while creating (during ingestion)
     * or synthesizing (at query time) the _id field.
     */
    public String createId(byte[] suffix, IntSupplier onEmpty) {
        byte[] idBytes = new byte[4 + suffix.length];
        ByteUtils.writeIntLE(buildHash(onEmpty), idBytes, 0);
        System.arraycopy(suffix, 0, idBytes, 4, suffix.length);
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(idBytes);
    }

    void extractObject(@Nullable String path, XContentParser source) throws IOException {
        while (source.currentToken() != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, source.currentToken(), source);
            String fieldName = source.currentName();
            String subPath = path == null ? fieldName : path + "." + fieldName;
            source.nextToken();
            extractItem(subPath, source);
        }
    }

    private void extractArray(@Nullable String path, XContentParser source) throws IOException {
        while (source.currentToken() != XContentParser.Token.END_ARRAY) {
            expectValueToken(source.currentToken(), source);
            extractItem(path, source);
        }
    }

    private void extractItem(String path, XContentParser source) throws IOException {
        switch (source.currentToken()) {
            case START_OBJECT:
                source.nextToken();
                extractObject(path, source);
                source.nextToken();
                break;
            case VALUE_STRING:
            case VALUE_NUMBER:
            case VALUE_BOOLEAN:
                XContentString.UTF8Bytes utf8Bytes = source.optimizedText().bytes();
                addHash(path, new BytesRef(utf8Bytes.bytes(), utf8Bytes.offset(), utf8Bytes.length()));
                source.nextToken();
                break;
            case START_ARRAY:
                source.nextToken();
                extractArray(path, source);
                source.nextToken();
                break;
            case VALUE_NULL:
                source.nextToken();
                break;
            default:
                throw new ParsingException(
                    source.getTokenLocation(),
                    "Cannot extract routing path due to unexpected token [{}]",
                    source.currentToken()
                );
        }
    }

    private void addHash(String path, BytesRef value) {
        hashes.add(new NameAndHash(new BytesRef(path), IndexRouting.ExtractFromSource.hash(value), hashes.size()));
    }

    int buildHash(IntSupplier onEmpty) {
        if (hashes.isEmpty()) {
            return onEmpty.getAsInt();
        }
        Collections.sort(hashes);
        int hash = 0;
        for (NameAndHash nah : hashes) {
            hash = 31 * hash + (IndexRouting.ExtractFromSource.hash(nah.name) ^ nah.hash);
        }
        return hash;
    }

    private record NameAndHash(BytesRef name, int hash, int order) implements Comparable<NameAndHash> {
        @Override
        public int compareTo(NameAndHash o) {
            int i = name.compareTo(o.name);
            if (i != 0) return i;
            // ensures array values are in the order as they appear in the source
            return Integer.compare(order, o.order);
        }
    }
}
