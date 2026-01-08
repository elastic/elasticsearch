/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.ReadAdvice;
import org.elasticsearch.index.StandardIOBehaviorHint;
import org.elasticsearch.index.store.FsDirectoryFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.BiFunction;

public class KnnFileUtils {

    static Directory getDirectory(Path indexPath) throws IOException {
        Directory dir = FSDirectory.open(indexPath);
        if (dir instanceof MMapDirectory mmapDir) {
            mmapDir.setReadAdvice(getReadAdviceFunc()); // enable madvise
            return new FsDirectoryFactory.HybridDirectory(NativeFSLockFactory.INSTANCE, mmapDir, 64);
        }
        return dir;
    }

    private static BiFunction<String, IOContext, Optional<ReadAdvice>> getReadAdviceFunc() {
        return (name, context) -> {
            if (context.hints().contains(StandardIOBehaviorHint.INSTANCE) || name.endsWith(".cfs")) {
                return Optional.of(ReadAdvice.NORMAL);
            }
            return MMapDirectory.ADVISE_BY_CONTEXT.apply(name, context);
        };
    }
}
