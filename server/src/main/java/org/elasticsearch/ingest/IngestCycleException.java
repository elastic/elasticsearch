package org.elasticsearch.ingest;

class IngestCycleException extends RuntimeException {
     IngestCycleException(String message) {
        super(message);
    }
}
