/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.utils;

import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulates a common pattern of trying to open a bunch of resources and then transferring ownership elsewhere on success,
 * but closing them on failure.
 */
public class TransferableCloseables implements Closeable {

    private boolean transferred = false;
    private final List<Closeable> closeables = new ArrayList<>();

    public <T extends Closeable> T add(T releasable) {
        assert transferred == false : "already transferred";
        closeables.add(releasable);
        return releasable;
    }

    public Closeable transfer() {
        assert transferred == false : "already transferred";
        transferred = true;
        Collections.reverse(closeables);
        return () -> IOUtils.close(closeables);
    }

    @Override
    public void close() throws IOException {
        if (transferred == false) {
            IOUtils.close(closeables);
        }
    }
}
