/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class FileReloadListenerTests extends ESTestCase {

    public void testCallback() {
        final CountDownLatch latch = new CountDownLatch(2);
        final FileReloadListener fileReloadListener = new FileReloadListener(PathUtils.get("foo", "bar"), latch::countDown);

        Consumer<Path> consumer = randomFrom(
            fileReloadListener::onFileCreated,
            fileReloadListener::onFileChanged,
            fileReloadListener::onFileDeleted
        );

        consumer.accept(PathUtils.get("foo", "bar"));
        assertThat(latch.getCount(), equalTo(1L));

        consumer.accept(PathUtils.get("fizz", "baz"));
        assertThat(latch.getCount(), equalTo(1L));

        consumer.accept(PathUtils.get("foo", "bar"));
        assertThat(latch.getCount(), equalTo(0L));
    }
}
