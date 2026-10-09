/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.repositories.blobstore.BlobStoreRepository.UploadFailureSource;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository.UploadStage;
import org.elasticsearch.snapshots.AbortedSnapshotException;
import org.elasticsearch.snapshots.PausedSnapshotException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.classifyUploadFailure;
import static org.hamcrest.Matchers.equalTo;

public class UploadFailureClassificationTests extends ESTestCase {

    public void testWriteFailureIsTheRepositorys() {
        assertThat(
            classifyUploadFailure(new IOException("boom"), UploadStage.WRITE_BLOB, false),
            equalTo(UploadFailureSource.REPOSITORY_WRITE)
        );
        assertThat(
            classifyUploadFailure(new RuntimeException("boom"), UploadStage.WRITE_BLOB, false),
            equalTo(UploadFailureSource.REPOSITORY_WRITE)
        );
    }

    public void testFailureReadingTheSourceIsTheSources() {
        // the repository fails too when the stream it uploads fails
        assertThat(classifyUploadFailure(new IOException("boom"), UploadStage.WRITE_BLOB, true), equalTo(UploadFailureSource.SOURCE_READ));
        // and so is not being able to open the source
        assertThat(
            classifyUploadFailure(new IOException("boom"), UploadStage.OPEN_SOURCE, false),
            equalTo(UploadFailureSource.SOURCE_READ)
        );
    }

    public void testSourceThatDoesNotVerifyIsNeither() {
        assertThat(classifyUploadFailure(new IOException("corrupt"), UploadStage.VERIFY_SOURCE, false), equalTo(UploadFailureSource.NONE));
    }

    public void testAbortsAndPausesAreNotFailures() {
        for (UploadStage stage : UploadStage.values()) {
            for (boolean sourceReadFailed : new boolean[] { true, false }) {
                assertThat(
                    classifyUploadFailure(new AbortedSnapshotException(), stage, sourceReadFailed),
                    equalTo(UploadFailureSource.NONE)
                );
                assertThat(
                    classifyUploadFailure(new PausedSnapshotException(), stage, sourceReadFailed),
                    equalTo(UploadFailureSource.NONE)
                );
                // also when the repository wrapped them
                assertThat(
                    classifyUploadFailure(new IOException("wrapped", new AbortedSnapshotException()), stage, sourceReadFailed),
                    equalTo(UploadFailureSource.NONE)
                );
            }
        }
    }
}
