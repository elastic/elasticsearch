/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.common.Strings;

import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 * The types of blobs in a {@link BlobStoreRepository}.
 */
public enum RepositoryFileType {

    ROOT_INDEX_N("index-NUM"),
    ROOT_INDEX_LATEST("index.latest"),
    SNAPSHOT_INFO("snap-UUID.dat"),
    GLOBAL_METADATA("meta-UUID.dat"),
    INDEX_METADATA("indices/UUID/meta-SHORTUUID.dat"),
    SHARD_GENERATION("indices/UUID/NUM/index-UUID"),
    SHARD_SNAPSHOT_INFO("indices/UUID/NUM/snap-UUID.dat"),
    SHARD_DATA("indices/UUID/NUM/__UUID"),
    // NB no support for legacy names (yet)
    ;

    private final Pattern pattern;

    RepositoryFileType(String regex) {
        pattern = Pattern.compile(
            "^("
                + regex
                    // decimal numbers
                    .replace("NUM", "(0|[1-9][0-9]*)")
                    // 15-byte UUIDS from TimeBasedUUIDGenerator
                    .replace("SHORTUUID", "[0-9a-zA-Z_-]{20}")
                    // 16-byte UUIDs from RandomBasedUUIDGenerator
                    .replace("UUID", "[0-9a-zA-Z_-]{22}")
                + ")$"
        );
    }

    public static RepositoryFileType getRepositoryFileType(Path repositoryRoot, Path blobPath) {
        final var relativePath = repositoryRoot.relativize(blobPath).toString().replace(repositoryRoot.getFileSystem().getSeparator(), "/");
        for (final var repositoryFileType : RepositoryFileType.values()) {
            if (repositoryFileType.pattern.matcher(relativePath).matches()) {
                return repositoryFileType;
            }
        }
        throw new IllegalArgumentException(
            Strings.format("[%s] is not the path of a known blob type within [%s]", relativePath, repositoryRoot)
        );
    }

}
