/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.customprocessing;

/**
 * A processor to manipulate rows before writing them to the process
 */
public interface CustomProcessor {

    void process(String[] row);
}
