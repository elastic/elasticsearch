/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class ModelDownloadStatusTests extends AbstractWireSerializingTestCase<ModelDownloadTask.DownloadStatus> {
    @Override
    protected Writeable.Reader<ModelDownloadTask.DownloadStatus> instanceReader() {
        return ModelDownloadTask.DownloadStatus::new;
    }

    @Override
    protected ModelDownloadTask.DownloadStatus createTestInstance() {
        return new ModelDownloadTask.DownloadStatus(
            new ModelDownloadTask.DownLoadProgress(randomIntBetween(1, 1000), randomIntBetween(0, 1000))
        );
    }

    @Override
    protected ModelDownloadTask.DownloadStatus mutateInstance(ModelDownloadTask.DownloadStatus instance) throws IOException {
        return new ModelDownloadTask.DownloadStatus(
            new ModelDownloadTask.DownLoadProgress(
                instance.downloadProgress().totalParts() + 1,
                instance.downloadProgress().downloadedParts() - 1
            )
        );
    }
}
