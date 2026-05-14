/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.matchAllQueryBytes;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.randomRemoteInfo;
import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;

public class RemoteInfoWireSerializingTests extends AbstractWireSerializingTestCase<RemoteInfo> {

    /**
     * {@link RemoteInfo} mutations must not share a {@link SecureString} with the instance being mutated: {@link #dispose} closes the
     * mutation and would invalidate the original before wire equality checks run.
     */
    private static SecureString copyPassword(@Nullable SecureString password) {
        return password == null ? null : password.clone();
    }

    /** Valid non-{@link QueryBuilders#matchAllQuery()} JSON for remote query */
    private static BytesReference termQueryBytes() {
        try {
            return BytesReference.bytes(
                QueryBuilders.termQuery("_id", "mutated").toXContent(XContentBuilder.builder(jsonXContent), ToXContent.EMPTY_PARAMS)
            );
        } catch (IOException ioException) {
            throw new AssertionError(ioException);
        }
    }

    /**
     * {@link BulkByScrollWireSerializingTestUtils#randomRemoteInfo()} always uses
     * {@link BulkByScrollWireSerializingTestUtils#matchAllQueryBytes()}; toggling with a different fixed query avoids
     * {@link ESTestCase#randomValueOtherThan} spinning forever when the supplier keeps returning the same bytes as the input.
     */
    private static BytesReference queryMutation(BytesReference current) {
        BytesReference matchAll = matchAllQueryBytes();
        BytesReference term = termQueryBytes();
        return current.equals(matchAll) ? term : matchAll;
    }

    @Override
    protected Writeable.Reader<RemoteInfo> instanceReader() {
        return RemoteInfo::new;
    }

    @Override
    protected RemoteInfo createTestInstance() {
        return randomRemoteInfo();
    }

    @Override
    protected RemoteInfo mutateInstance(RemoteInfo instance) throws IOException {
        return switch (between(0, 9)) {
            case 0 -> new RemoteInfo(
                randomValueOtherThan(instance.getScheme(), () -> randomFrom("http", "https")),
                instance.getHost(),
                instance.getPort(),
                instance.getPathPrefix(),
                instance.getQuery(),
                instance.getUsername(),
                copyPassword(instance.getPassword()),
                instance.getHeaders(),
                instance.getSocketTimeout(),
                instance.getConnectTimeout()
            );
            case 1 -> new RemoteInfo(
                instance.getScheme(),
                randomValueOtherThan(instance.getHost(), () -> randomAlphaOfLength(12)),
                instance.getPort(),
                instance.getPathPrefix(),
                instance.getQuery(),
                instance.getUsername(),
                copyPassword(instance.getPassword()),
                instance.getHeaders(),
                instance.getSocketTimeout(),
                instance.getConnectTimeout()
            );
            case 2 -> new RemoteInfo(
                instance.getScheme(),
                instance.getHost(),
                randomValueOtherThan(instance.getPort(), () -> between(1, 65535)),
                instance.getPathPrefix(),
                instance.getQuery(),
                instance.getUsername(),
                copyPassword(instance.getPassword()),
                instance.getHeaders(),
                instance.getSocketTimeout(),
                instance.getConnectTimeout()
            );
            case 3 -> new RemoteInfo(
                instance.getScheme(),
                instance.getHost(),
                instance.getPort(),
                randomValueOtherThan(instance.getPathPrefix(), () -> randomBoolean() ? null : randomAlphaOfLength(6)),
                instance.getQuery(),
                instance.getUsername(),
                copyPassword(instance.getPassword()),
                instance.getHeaders(),
                instance.getSocketTimeout(),
                instance.getConnectTimeout()
            );
            case 4 -> new RemoteInfo(
                instance.getScheme(),
                instance.getHost(),
                instance.getPort(),
                instance.getPathPrefix(),
                queryMutation(instance.getQuery()),
                instance.getUsername(),
                copyPassword(instance.getPassword()),
                instance.getHeaders(),
                instance.getSocketTimeout(),
                instance.getConnectTimeout()
            );
            case 5 -> new RemoteInfo(
                instance.getScheme(),
                instance.getHost(),
                instance.getPort(),
                instance.getPathPrefix(),
                instance.getQuery(),
                randomValueOtherThan(instance.getUsername(), () -> randomBoolean() ? randomAlphaOfLength(5) : null),
                copyPassword(instance.getPassword()),
                instance.getHeaders(),
                instance.getSocketTimeout(),
                instance.getConnectTimeout()
            );
            case 6 -> new RemoteInfo(
                instance.getScheme(),
                instance.getHost(),
                instance.getPort(),
                instance.getPathPrefix(),
                instance.getQuery(),
                instance.getUsername(),
                // SecureString equality is content-based: a clone with the same chars still equals the original.
                instance.getPassword() == null
                    ? new SecureString(randomAlphaOfLength(8).toCharArray())
                    : randomValueOtherThan(instance.getPassword(), () -> new SecureString(randomAlphaOfLength(9).toCharArray())),
                instance.getHeaders(),
                instance.getSocketTimeout(),
                instance.getConnectTimeout()
            );
            case 7 -> new RemoteInfo(
                instance.getScheme(),
                instance.getHost(),
                instance.getPort(),
                instance.getPathPrefix(),
                instance.getQuery(),
                instance.getUsername(),
                copyPassword(instance.getPassword()),
                randomValueOtherThan(instance.getHeaders(), () -> Map.of(randomAlphaOfLength(3), randomAlphaOfLength(4))),
                instance.getSocketTimeout(),
                instance.getConnectTimeout()
            );
            case 8 -> new RemoteInfo(
                instance.getScheme(),
                instance.getHost(),
                instance.getPort(),
                instance.getPathPrefix(),
                instance.getQuery(),
                instance.getUsername(),
                copyPassword(instance.getPassword()),
                instance.getHeaders(),
                randomValueOtherThan(instance.getSocketTimeout(), ESTestCase::randomTimeValue),
                instance.getConnectTimeout()
            );
            case 9 -> new RemoteInfo(
                instance.getScheme(),
                instance.getHost(),
                instance.getPort(),
                instance.getPathPrefix(),
                instance.getQuery(),
                instance.getUsername(),
                copyPassword(instance.getPassword()),
                instance.getHeaders(),
                instance.getSocketTimeout(),
                randomValueOtherThan(instance.getConnectTimeout(), ESTestCase::randomTimeValue)
            );
            default -> throw new AssertionError();
        };
    }

    /**
     * Releases the {@link RemoteInfo} secure string password rather than relying on GC eventually cleaning it up
     */
    @Override
    protected void dispose(RemoteInfo remoteInfo) {
        try {
            remoteInfo.close();
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }
}
