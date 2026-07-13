/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Exchanges provide the ability to split an execution into multiple pipelines.
 * Pipelines can be executed by different threads on the same or different nodes, allowing  parallel and distributed of processing of data.
 */
package org.elasticsearch.compute.operator.exchange;
