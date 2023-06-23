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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import org.elasticsearch.threadpool.ThreadPool;

/**
 * This class computes the current node indexing load
 */
public class IngestLoadProbe {
    private final ThreadPool threadPool;
    private final AverageWriteLoadSampler writeLoadSampler;

    public IngestLoadProbe(ThreadPool threadPool, AverageWriteLoadSampler writeLoadSampler) {
        this.threadPool = threadPool;
        this.writeLoadSampler = writeLoadSampler;
    }

    /**
     * Returns the current ingestion load (number of WRITE threads needed to cope with the current ingestion workload).
     * The value returned is within the range [0, number_of_processors]
     */
    public double getIngestionLoad() {
        // TODO: implement this
        return 1;
    }
}
