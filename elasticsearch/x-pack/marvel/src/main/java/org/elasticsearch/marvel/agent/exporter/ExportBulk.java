/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An export bulk holds one of more documents until it got flushed. The {@link ExportBulk#flush()} usually triggers the exporting of the
 * documents to their final destination.
 */
public abstract class ExportBulk {

    protected final String name;
    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZING);

    public ExportBulk(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Add documents to the exporting bulk
     */
    public void add(Collection<MonitoringDoc> docs) throws ExportException {
        if (state.get() == State.INITIALIZING) {
            doAdd(docs);
        }
    }

    protected abstract void doAdd(Collection<MonitoringDoc> docs) throws ExportException;

    /**
     * Flush the exporting bulk
     */
    public void flush() throws ExportException {
        if (state.compareAndSet(State.INITIALIZING, State.FLUSHING)) {
            doFlush();
        }
    }

    protected abstract void doFlush();

    /**
     * Close the exporting bulk
     */
    public void close(boolean flush) throws ExportException {
        if (state.getAndSet(State.CLOSED) != State.CLOSED) {

            ExportException exception = null;
            try {
                if (flush) {
                    doFlush();
                }
            } catch (ExportException e) {
                if (exception != null) {
                    exception.addSuppressed(e);
                } else {
                    exception = e;
                }
            } finally {
                try {
                    doClose();
                } catch (Exception e) {
                    if (exception != null) {
                        exception.addSuppressed(e);
                    } else {
                        exception = new ExportException("Exception when closing export bulk", e);
                    }
                }
            }

            // rethrow exception
            if (exception != null) {
                throw exception;
            }
        }
    }

    protected abstract void doClose() throws ExportException;

    protected boolean isClosed() {
        return state.get() == State.CLOSED;
    }

    /**
     * This class holds multiple export bulks exposed as a single compound bulk.
     */
    public static class Compound extends ExportBulk {

        private final Collection<ExportBulk> bulks;

        public Compound(Collection<ExportBulk> bulks) {
            super("all");
            this.bulks = bulks;
        }

        @Override
        protected void doAdd(Collection<MonitoringDoc> docs) throws ExportException {
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
        }

        @Override
        protected void doFlush() {
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
        protected void doClose() throws ExportException {
            ExportException exception = null;
            for (ExportBulk bulk : bulks) {
                try {
                    // We can close without flushing since doFlush()
                    // would have been called by the parent class
                    bulk.close(false);
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

    private enum State {
        INITIALIZING,
        FLUSHING,
        CLOSED
    }
}
