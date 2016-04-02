/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import java.util.Collection;

/**
 *
 */
public abstract class ExportBulk {

    protected final String name;

    public ExportBulk(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    public abstract ExportBulk add(Collection<MonitoringDoc> docs) throws ExportException;

    public abstract void flush() throws ExportException;

    public final void close(boolean flush) throws ExportException {
        ExportException exception = null;
        if (flush) {
            flush();
        }

        // now closing
        try {
            onClose();
        } catch (Exception e) {
            if (exception != null) {
                exception.addSuppressed(e);
            } else {
                exception = new ExportException("Exception when closing export bulk", e);
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
        public ExportBulk add(Collection<MonitoringDoc> docs) throws ExportException {
            ExportException exception = null;
            for (ExportBulk bulk : bulks) {
                try {
                    bulk.add(docs);
                } catch (ExportException e) {
                    if (exception == null) {
                        exception = new ExportException("failed to add documents to export bulks");
                    }
                    exception.addExportException(e);
                }
            }
            if (exception != null) {
                throw exception;
            }
            return this;
        }

        @Override
        public void flush() throws ExportException {
            ExportException exception = null;
            for (ExportBulk bulk : bulks) {
                try {
                    bulk.flush();
                } catch (ExportException e) {
                    if (exception == null) {
                        exception = new ExportException("failed to flush export bulks");
                    }
                    exception.addExportException(e);
                }
            }
            if (exception != null) {
                throw exception;
            }
        }

        @Override
        protected void onClose() throws Exception {
            ExportException exception = null;
            for (ExportBulk bulk : bulks) {
                try {
                    bulk.onClose();
                } catch (ExportException e) {
                    if (exception == null) {
                        exception = new ExportException("failed to close export bulks");
                    }
                    exception.addExportException(e);
                }
            }
            if (exception != null) {
                throw exception;
            }
        }
    }
}
