/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.ElasticsearchException;

import java.util.Arrays;
import java.util.Collection;

/**
 *
 */
public abstract class ExportBulk {

    protected final String name;

    public ExportBulk(String name) {
        this.name = name;
    }

    public ExportBulk add(MarvelDoc... docs) throws Exception {
        return add(Arrays.asList(docs));
    }

    @Override
    public String toString() {
        return name;
    }

    public abstract ExportBulk add(Collection<MarvelDoc> docs) throws Exception;

    public abstract void flush() throws Exception;

    public final void close(boolean flush) throws Exception {
        Exception exception = null;
        if (flush) {
            try {
                flush();
            } catch (Exception e) {
                exception = e;
            }
        }

        // now closing
        try {
            onClose();
        } catch (Exception e) {
            if (exception != null) {
                exception.addSuppressed(e);
            } else {
                exception = e;
            }
        }

        // rethrow exception
        if (exception != null) {
            throw exception;
        }
    }

    protected void onClose() throws Exception {
    }

    public static class Compound extends ExportBulk {

        private final Collection<ExportBulk> bulks;

        public Compound(Collection<ExportBulk> bulks) {
            super("all");
            this.bulks = bulks;
        }

        @Override
        public ExportBulk add(Collection<MarvelDoc> docs) throws Exception {
            for (ExportBulk bulk : bulks) {
                bulk.add(docs);
            }
            return this;
        }

        @Override
        public void flush() throws Exception {
            Exception exception = null;
            for (ExportBulk bulk : bulks) {
                try {
                    bulk.flush();
                } catch (Exception e) {
                    if (exception == null) {
                        exception = new ElasticsearchException("failed to flush exporter bulks");
                    }
                    exception.addSuppressed(new ElasticsearchException("failed to flush [{}] exporter bulk", e, bulk.name));
                }
            }
            if (exception != null) {
                throw exception;
            }
        }
    }
}
