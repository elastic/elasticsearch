/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.categorize;

import org.elasticsearch.xpack.ml.job.process.MlProcess;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.util.Iterator;

public interface CategorizeProcess extends MlProcess {

    /**
     * @return stream of categorize results.  (These are a subset of autodetect results, hence the return type.)
     */
    Iterator<AutodetectResult> readResults();
}
